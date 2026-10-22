"""Produtor -> Kafka -> processador -> PostgreSQL, com um Kafka de verdade.

Precisa de KAFKA_SERVIDORES e DATABASE_URL_TESTE. No CI, os dois sobem como serviços.
"""

from __future__ import annotations

import gzip
import os
from dataclasses import replace
from datetime import timedelta

import pytest
from confluent_kafka import Consumer, Producer
from conftest import trajeto

from onibus import config, kafka, processador, produtor
from onibus.fontes.gravacao import Gravacao
from onibus.posicao import Posicao

pytestmark = pytest.mark.integracao


@pytest.fixture
def ambiente(kafka_isolado, con, monkeypatch):
    monkeypatch.setattr(config, "DATABASE_URL", os.environ["DATABASE_URL_TESTE"])
    return con


@pytest.fixture
def gravacao(tmp_path):
    """Três ônibus em duas linhas, 12 buscas de 30 s."""
    onibus = [trajeto("A1", "474"), trajeto("B2", "474", kmh=18), trajeto("C3", "309", kmh=0)]
    arquivo = tmp_path / "g.jsonl.gz"
    with gzip.open(arquivo, "wb") as f:
        for i in range(12):
            recebido = onibus[0][i].registrado_em + timedelta(seconds=20)
            for pontos in onibus:
                f.write(replace(pontos[i], recebido_em=recebido).para_json() + b"\n")
    return arquivo


def _contagens(con):
    return con.execute(
        "select (select count(*) from posicoes), (select count(*) from posicao_atual),"
        " (select count(*) from janela_linha), (select coalesce(sum(onibus), 0) from janela_linha)"
    ).fetchone()


def test_fluxo_completo_e_mensagem_invalida_vai_para_o_topico_de_descarte(ambiente, gravacao):
    produtor.rodar(Gravacao(gravacao), intervalo=0, ciclos=12)
    kafka.garantir_topicos()
    lixo = Producer(kafka.produtor_config())
    lixo.produce(config.TOPICO_POSICOES, key=b"x", value=b"isto nao e uma posicao")
    lixo.flush(10)

    processador.rodar(ocioso_para=3)

    posicoes, atuais, janelas, _ = _contagens(ambiente)
    assert (posicoes, atuais) == (36, 3)
    assert janelas >= 2
    lotes = ambiente.execute("select sum(mensagens), sum(novas), sum(invalidas) from lotes").fetchone()
    assert tuple(lotes) == (37, 36, 1)

    leitor = Consumer(kafka.consumidor_config(grupo="leitor-" + config.GRUPO_PROCESSADOR))
    leitor.subscribe([config.TOPICO_INVALIDAS])
    mensagem = None
    for _ in range(30):
        mensagem = leitor.poll(1)
        if mensagem is not None and not mensagem.error():
            break
    leitor.close()
    assert mensagem is not None and mensagem.value() == b"isto nao e uma posicao"
    assert dict(mensagem.headers())["motivo"].startswith(b"JSONDecodeError")


def _janelas(con):
    return con.execute("select cidade, linha, inicio, onibus, amostras, velocidade_media, parados"
                       " from janela_linha order by 1, 2, 3").fetchall()


class ConsumidorQueCai(Consumer):
    """Grava o lote no banco e cai antes de confirmar o offset no Kafka."""

    def commit(self, *args, **kwargs):
        raise SystemExit("queda simulada")


def test_queda_antes_do_commit_reprocessa_sem_duplicar(ambiente, gravacao, monkeypatch):
    produtor.rodar(Gravacao(gravacao), intervalo=0, ciclos=12)

    monkeypatch.setattr(processador, "Consumer", ConsumidorQueCai)
    with pytest.raises(SystemExit):
        processador.rodar(ocioso_para=3)
    gravadas_antes_da_queda = _contagens(ambiente)[0]
    assert gravadas_antes_da_queda > 0

    # volta a rodar: o offset não foi confirmado, então o lote é lido de novo
    monkeypatch.setattr(processador, "Consumer", Consumer)
    processador.rodar(ocioso_para=3)
    posicoes, atuais, _, _ = _contagens(ambiente)
    assert (posicoes, atuais) == (36, 3)
    duplicadas = ambiente.execute("select sum(duplicadas) from lotes").fetchone()[0]
    assert duplicadas == gravadas_antes_da_queda  # o que já estava no banco foi reconhecido e ignorado

    # e as janelas ficaram iguais às de uma passada única e limpa pelas mesmas posições
    depois = _janelas(ambiente)
    linhas = ambiente.execute(
        "select cidade, veiculo, linha, lat, lon, registrado_em, recebido_em from posicoes"
    ).fetchall()
    ambiente.execute("truncate posicoes, posicao_atual, janela_linha, lotes")
    processador.gravar(ambiente, [
        Posicao(cidade=c, veiculo=v, linha=li, lat=la, lon=lo, registrado_em=r, recebido_em=rc)
        for c, v, li, la, lo, r, rc in linhas
    ])
    assert _janelas(ambiente) == depois
