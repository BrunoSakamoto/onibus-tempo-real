"""Gravação no PostgreSQL e cálculo das janelas (precisa de DATABASE_URL_TESTE)."""

from dataclasses import replace
from datetime import timedelta

import pytest
from conftest import T0, trajeto

from onibus import processador
from onibus.processador import gravar, inicio_da_janela, interpretar

pytestmark = pytest.mark.integracao


def _janelas(con, linha="474"):
    return con.execute(
        "select inicio, onibus, amostras, velocidade_media, parados from janela_linha "
        "where linha = %s order by inicio",
        [linha],
    ).fetchall()


def test_inicio_da_janela():
    assert inicio_da_janela(T0 + timedelta(minutes=7, seconds=12)) == T0 + timedelta(minutes=5)
    assert inicio_da_janela(T0) == T0


def test_velocidade_e_parados_por_janela(con):
    andando = trajeto("A1", kmh=30)  # 21 pontos de 30 s: 10 minutos, duas janelas
    parado = trajeto("P9", kmh=0)
    gravar(con, andando + parado)

    janelas = _janelas(con)
    assert [j[0] for j in janelas] == [T0, T0 + timedelta(minutes=5), T0 + timedelta(minutes=10)]
    primeira = janelas[0]
    assert primeira[1:3] == (2, 20)  # 2 ônibus, 10 pontos cada
    # média entre o que anda a 30 km/h e o parado; e só o parado conta como parado
    assert primeira[3] == pytest.approx(15.0, abs=0.2)
    assert primeira[4] == 1


def test_trecho_que_atravessa_a_janela_conta_na_janela_do_ponto_novo(con):
    pontos = trajeto("A1", kmh=36, pontos=11)  # termina exatamente às 12:05
    gravar(con, pontos)
    ultima = _janelas(con)[-1]
    # a janela das 12:05 só tem um ponto, mas o trecho desde 12:04:30 dá a velocidade dela
    assert ultima[0] == T0 + timedelta(minutes=5) and ultima[2] == 1
    assert ultima[3] == pytest.approx(36.0, abs=0.2)


def test_salto_de_gps_fica_fora_da_velocidade(con):
    pontos = trajeto("A1", kmh=30, pontos=10)
    pontos[5] = replace(pontos[5], lat=pontos[5].lat + 0.05)  # 5 km de salto em 30 s
    gravar(con, pontos)
    assert _janelas(con)[0][3] < 31


def test_lote_repetido_nao_muda_nada(con):
    pontos = trajeto("A1") + trajeto("B2", kmh=20)
    primeiro = gravar(con, pontos)
    antes = _janelas(con)
    segundo = gravar(con, pontos)
    assert (primeiro["novas"], primeiro["duplicadas"]) == (42, 0)
    assert (segundo["novas"], segundo["duplicadas"]) == (0, 42)
    assert _janelas(con) == antes
    assert con.execute("select count(*) from posicoes").fetchone()[0] == 42


def test_ordem_de_chegada_nao_muda_o_resultado(con):
    pontos = trajeto("A1") + trajeto("B2", kmh=20)
    gravar(con, pontos)
    em_ordem = _janelas(con)
    con.execute("truncate posicoes, janela_linha, posicao_atual")
    # chega em três lotes, embaralhado, com o meio atrasado
    gravar(con, pontos[:10] + pontos[30:])
    gravar(con, list(reversed(pontos[20:30])))
    gravar(con, pontos[10:20])
    assert _janelas(con) == em_ordem


def test_posicao_atual_nao_volta_no_tempo(con):
    pontos = trajeto("A1")
    gravar(con, pontos[5:])
    gravar(con, pontos[:5])  # atrasadas
    atual = con.execute("select registrado_em from posicao_atual where veiculo = 'A1'").fetchone()[0]
    assert atual == pontos[-1].registrado_em


def test_mensagens_invalidas_sao_separadas_com_o_motivo():
    boa = trajeto("A1")[0]
    validas, invalidas = interpretar([
        (b"k", boa.para_json()),
        (b"k", b"{nao e json"),
        (b"k", b'{"cidade": "rio"}'),
        (None, None),
        (b"k", replace(boa, lat=0.0).para_json()),
    ])
    assert validas == [boa]
    tipos = [i.motivo.split(":")[0] for i in invalidas]
    assert tipos == ["JSONDecodeError", "KeyError", "JSONDecodeError", "ValueError"]
    assert "fora da cidade" in invalidas[3].motivo


def test_limpeza_apaga_o_que_passou_da_retencao(con):
    gravar(con, trajeto("A1"))
    con.execute("update posicoes set registrado_em = registrado_em - interval '30 days'")
    con.execute("update posicao_atual set registrado_em = registrado_em - interval '30 days'")
    processador.limpar(con, horas=24)
    assert con.execute("select count(*) from posicoes").fetchone()[0] == 0
    assert con.execute("select count(*) from posicao_atual").fetchone()[0] == 0
