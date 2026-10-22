import gzip
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from conftest import T0, trajeto

from onibus.fontes.gravacao import Gravacao
from onibus.fontes.rio import Rio, ler_horario
from onibus.fontes.sptrans import SemToken, SPTrans
from onibus.posicao import Posicao, problema


def test_posicao_ida_e_volta_pelo_json():
    p = trajeto("A1")[3]
    assert Posicao.de_json(p.para_json()) == p
    assert p.chave == b"rio:A1"


@pytest.mark.parametrize(
    "mudanca, motivo",
    [
        ({"lat": 0.0, "lon": 0.0}, "fora da cidade"),
        ({"lat": -23.55, "lon": -46.63}, "fora da cidade"),  # São Paulo numa posição do Rio
        ({"registrado_em": T0 + timedelta(minutes=10)}, "horário no futuro"),
        ({"linha": ""}, "sem veículo ou linha"),
    ],
)
def test_posicoes_com_defeito_sao_recusadas(mudanca, motivo):
    p = replace(trajeto("A1")[0], **mudanca)
    assert problema(p) == motivo
    with pytest.raises(ValueError, match=motivo):
        Posicao.de_json(p.para_json())


def test_horario_sem_fuso_e_recusado():
    dados = json.loads(trajeto("A1")[0].para_json())
    dados["registrado_em"] = "2026-10-22T12:00:00"
    with pytest.raises(ValueError, match="fuso"):
        Posicao.de_json(json.dumps(dados).encode())


POSICAO_SPTRANS = {
    "hr": "09:30",
    "l": [
        {"c": "8000-10", "cl": 1273, "sl": 1, "lt0": "TERM. LAPA", "lt1": "PÇA. RAMOS", "qv": 2,
         "vs": [
             {"p": "11433", "a": True, "ta": "2026-10-22T12:29:40Z", "py": -23.5402, "px": -46.6411},
             {"p": "11434", "a": True, "ta": "2026-10-22T12:29:51Z", "py": 0, "px": 0},
         ]},
        {"c": "875A-10", "sl": 2, "vs": [{"p": "22001", "ta": "2026-10-22T12:29:55Z"}]},  # sem coordenadas
    ],
}


def _sptrans(tratar) -> SPTrans:
    return SPTrans("token-teste", httpx.Client(transport=httpx.MockTransport(tratar)))


def test_sptrans_converte_e_conta_descartes():
    agora = datetime(2026, 10, 22, 12, 30, tzinfo=UTC)
    fonte = SPTrans("token-teste")
    posicoes = fonte.converter(POSICAO_SPTRANS, agora)
    assert [(p.veiculo, p.linha, p.sentido, p.cidade) for p in posicoes] == [("11433", "8000-10", "1", "sp")]
    assert posicoes[0].registrado_em == datetime(2026, 10, 22, 12, 29, 40, tzinfo=UTC)
    assert fonte.descartes == {"fora da cidade": 1, "registro incompleto": 1}


def test_sptrans_autentica_e_refaz_quando_a_sessao_expira():
    pedidos = []

    def tratar(pedido: httpx.Request) -> httpx.Response:
        pedidos.append(pedido.url.path)
        if pedido.url.path.endswith("/Login/Autenticar"):
            assert pedido.url.params["token"] == "token-teste"
            return httpx.Response(200, json=True)
        if len([p for p in pedidos if p.endswith("/Posicao")]) == 2:  # segunda busca: sessão expirada
            return httpx.Response(401)
        return httpx.Response(200, json=POSICAO_SPTRANS)

    fonte = _sptrans(tratar)
    agora = datetime(2026, 10, 22, 12, 30, tzinfo=UTC)
    assert len(fonte.buscar(agora)) == 1
    assert len(fonte.buscar(agora)) == 1
    assert [p.rsplit("/", 1)[-1] for p in pedidos] == ["Autenticar", "Posicao", "Posicao", "Autenticar", "Posicao"]


def test_sptrans_sem_token_ou_com_token_recusado():
    with pytest.raises(SemToken, match="SPTRANS_TOKEN"):
        SPTrans("")
    fonte = _sptrans(lambda pedido: httpx.Response(200, json=False))
    with pytest.raises(SemToken, match="recusou"):
        fonte.buscar(datetime.now(UTC))


def test_gravacao_reproduz_os_ciclos_no_presente_e_recomeca(tmp_path):
    a, b = trajeto("A1", pontos=2), trajeto("B2", pontos=2)
    ciclo1 = [a[0], b[0]]
    ciclo2 = [a[1], b[1]]
    # os dois ciclos foram recebidos em momentos diferentes
    ciclo2 = [replace(p, recebido_em=ciclo1[0].recebido_em + timedelta(seconds=30)) for p in ciclo2]
    arquivo = tmp_path / "g.jsonl.gz"
    with gzip.open(arquivo, "wb") as f:
        for p in ciclo1 + ciclo2:
            f.write(p.para_json() + b"\n")

    fonte = Gravacao(arquivo)
    agora = datetime(2027, 1, 1, tzinfo=UTC)
    primeiro = fonte.buscar(agora)
    segundo = fonte.buscar(agora + timedelta(seconds=30))
    terceiro = fonte.buscar(agora + timedelta(seconds=60))

    assert [p.veiculo for p in primeiro] == ["A1", "B2"]
    assert all(p.recebido_em == agora for p in primeiro)
    # o atraso entre o GPS e a busca é preservado
    assert primeiro[0].recebido_em - primeiro[0].registrado_em == ciclo1[0].recebido_em - ciclo1[0].registrado_em
    assert segundo[0].registrado_em > primeiro[0].registrado_em
    assert terceiro[0].lat == ciclo1[0].lat and terceiro[0].recebido_em == agora + timedelta(seconds=60)


# ---------- Rio ----------

AMOSTRA_RIO = json.loads((Path(__file__).parent / "rio_amostra.json").read_text(encoding="utf-8"))


def test_horario_do_rio_esta_em_brasilia_apesar_do_z():
    assert ler_horario("2026-10-22T09:26:40Z") == datetime(2026, 10, 22, 12, 26, 40, tzinfo=UTC)


def test_rio_converte_a_resposta_real():
    agora = datetime(2026, 10, 22, 12, 30, tzinfo=UTC)
    fonte = Rio()
    registros = AMOSTRA_RIO + [dict(AMOSTRA_RIO[0], latitude=0, longitude=0), {"servico": "100"}]
    posicoes = fonte.converter(registros, agora)
    assert len(posicoes) == len(AMOSTRA_RIO)
    assert fonte.descartes == {"fora da cidade": 1, "registro incompleto": 1}
    primeira = posicoes[0]
    assert (primeira.veiculo, primeira.linha, primeira.sentido) == ("C50020", "810", None)  # sentido "" vira nulo
    assert primeira.registrado_em == datetime(2026, 10, 22, 12, 26, 43, tzinfo=UTC)
    assert all(problema(p) is None and p.cidade == "rio" for p in posicoes)


def test_rio_pede_desde_a_busca_anterior_com_sobreposicao():
    pedidos = []
    falhar = {"agora": False}

    def tratar(pedido: httpx.Request) -> httpx.Response:
        pedidos.append((pedido.url.params["dataInicial"], pedido.url.params["dataFinal"]))
        return httpx.Response(503) if falhar["agora"] else httpx.Response(200, json=AMOSTRA_RIO)

    fonte = Rio(httpx.Client(transport=httpx.MockTransport(tratar)))
    t = datetime(2026, 10, 22, 12, 30, tzinfo=UTC)
    fonte.buscar(t)
    fonte.buscar(t + timedelta(seconds=30))
    falhar["agora"] = True
    with pytest.raises(httpx.HTTPStatusError):
        fonte.buscar(t + timedelta(seconds=60))
    falhar["agora"] = False
    fonte.buscar(t + timedelta(seconds=90))  # cobre também o intervalo da busca que falhou
    fonte.buscar(t + timedelta(hours=2))  # depois de uma queda longa, só os últimos 10 min

    assert pedidos == [
        ("2026-10-22T12:27:00Z", "2026-10-22T12:30:00Z"),
        ("2026-10-22T12:29:30Z", "2026-10-22T12:30:30Z"),
        ("2026-10-22T12:30:00Z", "2026-10-22T12:31:00Z"),
        ("2026-10-22T12:30:00Z", "2026-10-22T12:31:30Z"),
        ("2026-10-22T14:20:00Z", "2026-10-22T14:30:00Z"),
    ]
