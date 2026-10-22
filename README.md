# Ônibus em tempo real · streaming com Kafka

Acompanha os cerca de 3.500 ônibus municipais do Rio de Janeiro em tempo real. Um produtor lê a
API pública de GPS da Prefeitura a cada 30 segundos e publica cada posição no Kafka. Um
processador consome o tópico, grava no PostgreSQL e calcula, por linha e em janelas de 5
minutos, quantos ônibus estão rodando, a velocidade média e quantos estão parados. Um painel
mostra tudo num mapa ao vivo.

**Python · Apache Kafka · PostgreSQL · FastAPI · MapLibre · Docker · GitHub Actions**

![Painel com o mapa dos ônibus do Rio e as linhas mais lentas na última janela](docs/painel.jpg)
<sub>Captura com dados reais de 22/10/2026, às 10h, sem o mapa de ruas de fundo. Cada ponto é um ônibus.</sub>

## Arquitetura

```mermaid
flowchart LR
    API[API de GPS<br/>Prefeitura do Rio] -->|a cada 30 s| P[Produtor]
    P -->|chave = veículo| K[(Kafka<br/>tópico posicoes)]
    K --> C[Processador]
    C -->|inválidas| D[(tópico<br/>posicoes-invalidas)]
    C -->|uma transação por lote| PG[(PostgreSQL)]
    PG --> W[Painel FastAPI + mapa]
```

| Serviço | O que faz |
| --- | --- |
| `produtor` | Busca as posições desde a busca anterior, converte para um formato comum, descarta as que já publicou e publica no tópico `posicoes`, com o veículo como chave. |
| `processador` | Lê em lotes, separa as mensagens inválidas num tópico de descarte, grava as posições, atualiza a posição atual de cada ônibus e recalcula as janelas afetadas. Só confirma o offset no Kafka depois que o banco confirmou. |
| `painel` | API em FastAPI que lê do PostgreSQL e uma página com o mapa (MapLibre), as linhas mais lentas e a saúde do pipeline. |

## O que os dados reais mostraram

O formato da API foi definido olhando a resposta de verdade, não a documentação, e isso mudou
o projeto em três pontos:

- **O "Z" mente.** O intervalo pedido é em UTC, mas os horários da resposta estão no horário de
  Brasília com um `Z` no fim, como se fossem UTC. Lidos ao pé da letra, todos os ônibus
  estariam 3 horas no futuro. O adaptador converte, e um teste fixa esse comportamento.
- **Os pontos chegam atrasados, e fora de ordem.** A API filtra pelo horário em que o ponto
  chegou ao servidor, não pelo horário do GPS. Numa gravação de 30 minutos, metade das posições
  chegou ao produtor até 29 s depois do horário do GPS, e 5% levaram mais de 1 minuto. Isso
  bastou para que **16% das posições chegassem depois que a janela de 5 minutos delas já tinha
  terminado**. Uma janela fechada pelo relógio perderia esses pontos. Por isso as janelas são
  por **tempo do evento** (o instante do GPS) e um ponto atrasado corrige a janela dele.
- **Nem todo ônibus está em linha.** Há veículos com serviço `GARAGEM`, `MANUTENCAO` e
  `ESPECIAL`. Eles aparecem no mapa, mas ficam fora das médias e do ranking.

Volume: cerca de 3.500 ônibus ao mesmo tempo e 9 mil posições por minuto (270 mil em meia
hora), com mediana de 3 s entre a busca na API e o dado no banco. Algumas posições vêm com
coordenadas fora do município e são descartadas já no produtor.

## Decisões de engenharia

- **Efeito de "exatamente uma vez" com entrega "pelo menos uma vez".** O processador grava
  cada lote numa transação e só depois confirma o offset. Se cair no meio, relê o lote; como a
  posição tem chave (cidade, veículo, instante) e a gravação ignora o que já existe, reler não
  duplica nada. Há um teste que derruba o processador entre a gravação e o commit e confere
  que o resultado final é idêntico ao de uma passada única.
- **Janelas recalculadas a partir das posições, não acumuladas em memória.** Cada lote
  recalcula, em SQL, só as janelas que tocou (e a seguinte, porque o trecho que sai de um ponto
  conta na janela do próximo). Assim o processador não guarda estado em memória e pode
  reiniciar a qualquer momento sem perder nada.
- **Velocidade calculada, não informada.** A velocidade que o veículo informa é zero em 28% dos
  pontos, e em 3 de cada 10 desses o ônibus andou a mais de 10 km/h desde o ponto anterior.
  A do projeto é a distância entre pontos de GPS seguidos (haversine) dividida pelo tempo, ponderada pelo tempo de cada ônibus na janela. Saltos de GPS
  acima de 120 km/h e intervalos maiores que 10 minutos ficam de fora.
- **Ordem por veículo.** A chave da mensagem é o veículo, então todas as posições de um ônibus
  caem na mesma partição, em ordem.
- **Mensagens ruins não travam o consumo.** JSON quebrado, campo faltando, coordenada fora da
  cidade ou horário no futuro vão para `posicoes-invalidas`, com o motivo num cabeçalho.
- **Produtor que não perde dados nas falhas da API.** Cada busca pede o intervalo desde a última
  que deu certo, com 30 segundos de sobreposição. Se uma busca falha, a seguinte cobre o
  buraco, até um limite de 10 minutos. O produtor lembra o que publicou nos últimos 15 minutos
  para não repetir, sem deixar a memória crescer.
- **Fonte trocável.** Rio, SPTrans (API Olho Vivo, com token) e uma gravação que reproduz dados
  reais como se fossem ao vivo. O resto do pipeline só conhece o formato comum.
- **Retenção.** O tópico guarda 24 horas; o banco, 6 horas de posições e 7 dias de janelas.

## Como rodar

Precisa de Docker.

```bash
git clone https://github.com/BrunoSakamoto/onibus-tempo-real
cd onibus-tempo-real
docker compose up --build
```

Abra http://localhost:8000. Os primeiros ônibus aparecem em menos de um minuto; as médias por
linha, quando a primeira janela de 5 minutos fecha.

**Sem internet**, ou se a API estiver fora do ar, reproduza a gravação de dados reais que vem no
projeto:

```bash
FONTE=gravacao docker compose up --build        # Windows (PowerShell): $env:FONTE="gravacao"
```

**São Paulo:** crie um token em [sptrans.com.br/desenvolvedores](https://www.sptrans.com.br/desenvolvedores/),
copie `.env.example` para `.env`, preencha `SPTRANS_TOKEN` e use `FONTE=sptrans`.

**Em desenvolvimento**, com o Kafka e o PostgreSQL do Compose:

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
docker compose up -d kafka postgres
export KAFKA_SERVIDORES=localhost:9094 DATABASE_URL=postgresql://onibus:onibus@localhost:5432/onibus
python -m onibus produtor      # num terminal
python -m onibus processador   # em outro
python -m onibus painel        # e em outro
```

## Testes

```bash
pytest -v
```

32 testes. Os de unidade cobrem os adaptadores (inclusive com uma amostra da resposta
real do Rio e a reautenticação da SPTrans), a deduplicação do produtor e a gravação. Os de
integração usam PostgreSQL e Kafka de verdade (variáveis `DATABASE_URL_TESTE` e
`KAFKA_SERVIDORES`; sem elas, são pulados):

- velocidade e ônibus parados por janela, trecho que atravessa duas janelas, salto de GPS;
- lote repetido não muda nada; posições chegando fora de ordem dão o mesmo resultado;
- fluxo completo produtor → Kafka → processador → PostgreSQL, com uma mensagem inválida indo
  para o tópico de descarte;
- queda do processador entre a gravação e o commit do offset, sem duplicar nem perder nada;
- a API do painel.

O GitHub Actions sobe Kafka e PostgreSQL como serviços, roda tudo e constrói a imagem Docker a
cada push.

## Estrutura

```
onibus/
  posicao.py      o evento: formato comum e validação
  fontes/         rio.py, sptrans.py e gravacao.py
  produtor.py     busca, deduplica e publica no Kafka
  processador.py  consome em lotes, grava e recalcula as janelas
  banco.py        esquema e SQL (inclusive o cálculo das janelas)
  painel.py       API do painel
  estatico/       página do mapa
gravacoes/        dados reais gravados, para rodar sem internet
tests/
```

---

Feito por [Bruno Araújo Sakamoto](https://www.linkedin.com/in/bruno-a-sakamoto/).
