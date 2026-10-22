"""Processador: consome as posições do Kafka, grava no PostgreSQL e mantém as janelas por linha.

Garantia: cada lote é gravado numa transação e só depois o offset é confirmado no Kafka. Se o
processo cair no meio, o lote é lido de novo (entrega "pelo menos uma vez"), e como a gravação
ignora posições que já existem e as janelas são recalculadas do zero, o resultado é o mesmo de
uma única leitura. Mensagens que não são posições válidas vão para um tópico separado, com o
motivo no cabeçalho, em vez de travar o consumo.
"""

from __future__ import annotations

import json
import logging
import signal
import statistics
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg
from confluent_kafka import Consumer, Producer

from . import banco, config, kafka
from .posicao import Posicao

log = logging.getLogger(__name__)

JANELA = timedelta(minutes=config.JANELA_MINUTOS)
_EPOCA = datetime(2000, 1, 1, tzinfo=UTC)


def inicio_da_janela(momento: datetime) -> datetime:
    return momento - (momento - _EPOCA) % JANELA


@dataclass
class Invalida:
    chave: bytes | None
    valor: bytes | None
    motivo: str


def interpretar(valores: list[tuple[bytes | None, bytes | None]]) -> tuple[list[Posicao], list[Invalida]]:
    validas, invalidas = [], []
    for chave, valor in valores:
        try:
            validas.append(Posicao.de_json(valor or b""))
        except (ValueError, TypeError, KeyError) as erro:  # JSON quebrado, campo faltando, dado ruim
            invalidas.append(Invalida(chave, valor, f"{type(erro).__name__}: {erro}"[:200]))
    return validas, invalidas


def gravar(con: psycopg.Connection, posicoes: list[Posicao], invalidas: int = 0) -> dict:
    """Grava um lote numa transação: posições novas, posição atual e janelas afetadas."""
    agora = datetime.now(UTC)
    colunas = {
        "cidade": [p.cidade for p in posicoes],
        "veiculo": [p.veiculo for p in posicoes],
        "registrado_em": [p.registrado_em for p in posicoes],
        "linha": [p.linha for p in posicoes],
        "lat": [p.lat for p in posicoes],
        "lon": [p.lon for p in posicoes],
        "velocidade": [p.velocidade for p in posicoes],
        "sentido": [p.sentido for p in posicoes],
        "recebido_em": [p.recebido_em for p in posicoes],
    }
    ultimas: dict[tuple[str, str], Posicao] = {}
    for p in posicoes:
        atual = ultimas.get((p.cidade, p.veiculo))
        if atual is None or p.registrado_em > atual.registrado_em:
            ultimas[(p.cidade, p.veiculo)] = p
    # a janela de cada posição e a seguinte (o trecho que sai deste ponto conta na próxima)
    janelas = {
        (p.cidade, p.linha, inicio_da_janela(p.registrado_em) + desloca)
        for p in posicoes
        for desloca in (timedelta(0), JANELA)
    }
    with con.transaction():
        with con.cursor() as cur:
            cur.execute(banco.INSERIR_POSICOES, colunas)
            novas = len(cur.fetchall()) if posicoes else 0
        con.execute(banco.ATUALIZAR_POSICAO_ATUAL, {
            "cidade": [p.cidade for p in ultimas.values()],
            "veiculo": [p.veiculo for p in ultimas.values()],
            "linha": [p.linha for p in ultimas.values()],
            "lat": [p.lat for p in ultimas.values()],
            "lon": [p.lon for p in ultimas.values()],
            "velocidade": [p.velocidade for p in ultimas.values()],
            "registrado_em": [p.registrado_em for p in ultimas.values()],
        })
        if novas:  # lote só de repetidas não muda janela nenhuma
            con.execute(banco.RECALCULAR_JANELAS, {
                "cidade": [j[0] for j in janelas],
                "linha": [j[1] for j in janelas],
                "inicio": [j[2] for j in janelas],
                "minutos": config.JANELA_MINUTOS,
            })
        resumo = {
            "mensagens": len(posicoes) + invalidas,
            "novas": novas,
            "duplicadas": len(posicoes) - novas,
            "invalidas": invalidas,
            "janelas": len(janelas) if novas else 0,
            "atraso_fonte_s": _mediana([(p.recebido_em - p.registrado_em).total_seconds() for p in posicoes]),
            "atraso_pipeline_s": _mediana([(agora - p.recebido_em).total_seconds() for p in posicoes]),
        }
        con.execute(banco.REGISTRAR_LOTE, resumo)
    return resumo


def _mediana(valores: list[float]) -> float | None:
    return round(statistics.median(valores), 1) if valores else None


def limpar(con: psycopg.Connection, horas: int | None = None) -> None:
    with con.transaction():
        for comando in banco.LIMPAR:
            con.execute(comando, {"horas": horas or config.RETENCAO_HORAS})


def rodar(max_lotes: int | None = None, ocioso_para: float | None = None) -> None:
    """Laço principal. `max_lotes` e `ocioso_para` (segundos sem mensagens) servem aos testes."""
    kafka.garantir_topicos()
    con = banco.conectar(config.DATABASE_URL)
    banco.criar_esquema(con)
    consumidor = Consumer(kafka.consumidor_config())
    consumidor.subscribe([config.TOPICO_POSICOES])
    descartes = Producer(kafka.produtor_config())
    parar = False

    def ao_sinal(*_):
        nonlocal parar
        parar = True

    signal.signal(signal.SIGTERM, ao_sinal)
    signal.signal(signal.SIGINT, ao_sinal)
    lotes, ultima_limpeza, ultima_mensagem = 0, 0.0, time.monotonic()
    try:
        while not parar and (max_lotes is None or lotes < max_lotes):
            mensagens = consumidor.consume(num_messages=2000, timeout=1.0)
            if time.monotonic() - ultima_limpeza > 600:
                limpar(con)
                ultima_limpeza = time.monotonic()
            if not mensagens:
                if ocioso_para is not None and time.monotonic() - ultima_mensagem > ocioso_para:
                    break
                continue
            ultima_mensagem = time.monotonic()
            for m in mensagens:
                if m.error():
                    log.warning("erro do Kafka: %s", m.error())
            validas, invalidas = interpretar([(m.key(), m.value()) for m in mensagens if not m.error()])
            for inv in invalidas:
                descartes.produce(
                    config.TOPICO_INVALIDAS, key=inv.chave, value=inv.valor,
                    headers={"motivo": inv.motivo.encode()},
                )
            if invalidas and descartes.flush(30):
                raise RuntimeError("não foi possível publicar as mensagens inválidas")
            resumo = gravar(con, validas, len(invalidas))
            consumidor.commit(asynchronous=False)  # só agora: o lote já está no banco
            lotes += 1
            log.info("lote: %s", json.dumps(resumo))
    finally:
        consumidor.close()
        con.close()
        log.info("processador encerrado")
