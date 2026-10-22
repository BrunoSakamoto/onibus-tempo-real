"""Esquema do PostgreSQL e as consultas de gravação.

- `posicoes`: cada posição recebida, uma vez só (chave: cidade, veículo, instante do GPS).
  É o que torna o processamento idempotente: a mesma mensagem lida duas vezes não muda nada.
- `posicao_atual`: a última posição de cada ônibus, para o mapa.
- `janela_linha`: por linha e janela de 5 minutos, quantos ônibus rodaram, a velocidade média
  e quantos ficaram parados. É sempre recalculada a partir de `posicoes`, então uma posição
  atrasada simplesmente corrige a janela dela.
- `lotes`: o que o processador fez em cada lote, para acompanhar a saúde do pipeline.
"""

from __future__ import annotations

import psycopg

ESQUEMA = """
create table if not exists posicoes (
    cidade         text             not null,
    veiculo        text             not null,
    registrado_em  timestamptz      not null,
    linha          text             not null,
    lat            double precision not null,
    lon            double precision not null,
    velocidade     real,
    sentido        text,
    recebido_em    timestamptz      not null,
    processado_em  timestamptz      not null default now(),
    primary key (cidade, veiculo, registrado_em)
);
create index if not exists posicoes_por_linha on posicoes (cidade, linha, registrado_em);

create table if not exists posicao_atual (
    cidade         text             not null,
    veiculo        text             not null,
    linha          text             not null,
    lat            double precision not null,
    lon            double precision not null,
    velocidade     real,
    registrado_em  timestamptz      not null,
    primary key (cidade, veiculo)
);

create table if not exists janela_linha (
    cidade            text        not null,
    linha             text        not null,
    inicio            timestamptz not null,
    onibus            int         not null,
    amostras          int         not null,
    velocidade_media  real,
    parados           int         not null,
    atualizado_em     timestamptz not null default now(),
    primary key (cidade, linha, inicio)
);
create index if not exists janela_por_inicio on janela_linha (inicio);

create table if not exists lotes (
    instante            timestamptz not null default now(),
    mensagens           int         not null,
    novas               int         not null,
    duplicadas          int         not null,
    invalidas           int         not null,
    janelas             int         not null,
    atraso_fonte_s      real,
    atraso_pipeline_s   real
);
create index if not exists lotes_por_instante on lotes (instante);

-- distância em metros entre dois pontos (haversine)
create or replace function distancia_m(lat1 float8, lon1 float8, lat2 float8, lon2 float8)
returns float8 language sql immutable parallel safe as $$
    select 2 * 6371000 * asin(sqrt(
        power(sin(radians(lat2 - lat1) / 2), 2)
        + cos(radians(lat1)) * cos(radians(lat2)) * power(sin(radians(lon2 - lon1) / 2), 2)
    ))
$$;
"""

INSERIR_POSICOES = """
insert into posicoes (cidade, veiculo, registrado_em, linha, lat, lon, velocidade, sentido, recebido_em)
select * from unnest(
    %(cidade)s::text[], %(veiculo)s::text[], %(registrado_em)s::timestamptz[], %(linha)s::text[],
    %(lat)s::float8[], %(lon)s::float8[], %(velocidade)s::real[], %(sentido)s::text[],
    %(recebido_em)s::timestamptz[]
)
on conflict do nothing
returning 1
"""

ATUALIZAR_POSICAO_ATUAL = """
insert into posicao_atual (cidade, veiculo, linha, lat, lon, velocidade, registrado_em)
select * from unnest(
    %(cidade)s::text[], %(veiculo)s::text[], %(linha)s::text[],
    %(lat)s::float8[], %(lon)s::float8[], %(velocidade)s::real[], %(registrado_em)s::timestamptz[]
)
on conflict (cidade, veiculo) do update set
    linha = excluded.linha, lat = excluded.lat, lon = excluded.lon,
    velocidade = excluded.velocidade, registrado_em = excluded.registrado_em
where posicao_atual.registrado_em < excluded.registrado_em
"""

# Recalcula as janelas indicadas a partir das posições. A velocidade de cada ônibus é a
# distância percorrida dividida pelo tempo, ponto a ponto; o trecho que liga ao ponto anterior
# (mesmo que ele esteja na janela de antes) conta na janela do ponto mais novo. Trechos com
# mais de 10 min entre os pontos ou acima de 120 km/h são salto de GPS e ficam de fora.
RECALCULAR_JANELAS = """
with alvo as (
    select distinct cidade, linha, inicio
    from unnest(%(cidade)s::text[], %(linha)s::text[], %(inicio)s::timestamptz[]) as a(cidade, linha, inicio)
),
pontos as (
    select a.cidade, a.linha, a.inicio, p.veiculo, p.registrado_em, p.lat, p.lon,
           lag(p.lat) over w as lat0, lag(p.lon) over w as lon0, lag(p.registrado_em) over w as t0
    from alvo a
    join posicoes p
      on p.cidade = a.cidade and p.linha = a.linha
     and p.registrado_em >= a.inicio - interval '10 minutes'
     and p.registrado_em < a.inicio + make_interval(mins => %(minutos)s)
    window w as (partition by a.cidade, a.linha, a.inicio, p.veiculo order by p.registrado_em)
),
na_janela as (
    select * from pontos where registrado_em >= inicio
),
trechos as (
    select cidade, linha, inicio, veiculo,
           distancia_m(lat0, lon0, lat, lon) as metros,
           extract(epoch from registrado_em - t0) as segundos
    from na_janela
    where t0 is not null and registrado_em - t0 <= interval '10 minutes'
),
por_veiculo as (
    select cidade, linha, inicio, veiculo, sum(metros) / sum(segundos) * 3.6 as kmh
    from trechos
    where segundos > 0 and metros / segundos * 3.6 <= 120
    group by cidade, linha, inicio, veiculo
),
contagem as (
    select cidade, linha, inicio, count(distinct veiculo) as onibus, count(*) as amostras
    from na_janela
    group by cidade, linha, inicio
)
insert into janela_linha (cidade, linha, inicio, onibus, amostras, velocidade_media, parados, atualizado_em)
select c.cidade, c.linha, c.inicio, c.onibus, c.amostras,
       round(avg(v.kmh)::numeric, 1),
       count(v.veiculo) filter (where v.kmh < 3),
       now()
from contagem c
left join por_veiculo v using (cidade, linha, inicio)
group by c.cidade, c.linha, c.inicio, c.onibus, c.amostras
on conflict (cidade, linha, inicio) do update set
    onibus = excluded.onibus, amostras = excluded.amostras,
    velocidade_media = excluded.velocidade_media, parados = excluded.parados,
    atualizado_em = excluded.atualizado_em
"""

REGISTRAR_LOTE = """
insert into lotes (mensagens, novas, duplicadas, invalidas, janelas, atraso_fonte_s, atraso_pipeline_s)
values (%(mensagens)s, %(novas)s, %(duplicadas)s, %(invalidas)s, %(janelas)s,
        %(atraso_fonte_s)s, %(atraso_pipeline_s)s)
"""

LIMPAR = [
    "delete from posicoes where registrado_em < now() - make_interval(hours => %(horas)s)",
    "delete from posicao_atual where registrado_em < now() - make_interval(hours => %(horas)s)",
    "delete from janela_linha where inicio < now() - interval '7 days'",
    "delete from lotes where instante < now() - interval '7 days'",
]


def conectar(url: str) -> psycopg.Connection:
    # autocommit + blocos `con.transaction()` explícitos: cada lote é uma transação, sem
    # transação implícita aberta entre um lote e outro.
    return psycopg.connect(url, autocommit=True)


def criar_esquema(con: psycopg.Connection) -> None:
    with con.transaction():
        con.execute(ESQUEMA)
