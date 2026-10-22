from collections import Counter
from datetime import timedelta

from conftest import T0, trajeto

from onibus.fontes.gravacao import Gravacao
from onibus.posicao import Posicao
from onibus.produtor import Publicador


class ProducerFalso:
    def __init__(self):
        self.mensagens = []

    def produce(self, topico, key, value, on_delivery):
        self.mensagens.append((topico, key, value))
        on_delivery(None, None)

    def poll(self, _):
        return 0

    def flush(self, _):
        return 0


class FonteFixa:
    """Devolve a lista seguinte a cada busca, como a API faria."""

    cidade = "rio"

    def __init__(self, ciclos):
        self.ciclos = list(ciclos)
        self.descartes = Counter()

    def buscar(self, agora):
        return self.ciclos.pop(0)


def test_nao_repete_o_que_ja_publicou_mas_publica_posicao_atrasada():
    a, b = trajeto("A1"), trajeto("B2")
    # buscas sobrepostas repetem posições; a[2] chega atrasada, depois de a[3]
    fonte = FonteFixa([[a[0], b[0]], [a[0], a[1], b[1]], [a[1], a[3], b[1]], [a[3], a[2]]])
    producer = ProducerFalso()
    pub = Publicador(fonte, producer, topico="t")

    resumos = [pub.ciclo(T0 + timedelta(seconds=30 * i)) for i in range(4)]

    assert [r["novas"] for r in resumos] == [2, 2, 1, 1]
    assert [Posicao.de_json(v).registrado_em for _, k, v in producer.mensagens if k == b"rio:A1"] == [
        a[0].registrado_em, a[1].registrado_em, a[3].registrado_em, a[2].registrado_em,
    ]
    assert all(t == "t" for t, _, _ in producer.mensagens)


def test_esquece_o_publicado_depois_de_um_tempo():
    a = trajeto("A1")
    pub = Publicador(FonteFixa([[a[0]], [a[0]], [a[0]]]), ProducerFalso(), topico="t")
    assert pub.ciclo(T0)["novas"] == 1
    assert pub.ciclo(T0 + timedelta(minutes=5))["novas"] == 0
    assert pub.ciclo(T0 + timedelta(minutes=30))["novas"] == 1  # memória limitada: não cresce sem fim


def test_gravar_e_reproduzir_devolve_as_mesmas_posicoes(tmp_path):
    a, b = trajeto("A1"), trajeto("B2")
    arquivo = tmp_path / "g.jsonl.gz"
    pub = Publicador(FonteFixa([[a[0], b[0]], [a[1], b[1]]]), ProducerFalso(), topico="t", gravar=arquivo)
    pub.ciclo(a[0].recebido_em)
    pub.ciclo(a[1].recebido_em)
    pub.fechar()

    fonte = Gravacao(arquivo)
    assert len(fonte.ciclos) == 2
    assert sorted(p.veiculo for p in fonte.ciclos[1]) == ["A1", "B2"]
    assert fonte.ciclos[1][0].lat == a[1].lat
