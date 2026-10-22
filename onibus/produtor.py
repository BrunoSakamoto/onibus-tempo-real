"""Produtor: busca as posições na fonte a cada intervalo e publica as novas no Kafka.

As buscas se sobrepõem (para não perder pontos na borda do intervalo), então a mesma posição
pode vir em buscas seguidas. O produtor lembra o que publicou nos últimos minutos e não repete.
Posições atrasadas, mais antigas que a última daquele ônibus, são publicadas normalmente: o
processador as coloca na janela certa. Se o produtor reiniciar e repetir algumas, tudo bem: o
processador grava de forma idempotente.
"""

from __future__ import annotations

import gzip
import logging
import signal
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from confluent_kafka import Producer

from . import config, kafka
from .fontes import Fonte
from .posicao import Posicao

log = logging.getLogger(__name__)

MEMORIA = timedelta(minutes=15)  # por quanto tempo lembrar de uma posição já publicada


class Publicador:
    def __init__(self, fonte: Fonte, producer, topico: str | None = None, gravar: Path | None = None):
        self.fonte = fonte
        self.producer = producer
        self.topico = topico or config.TOPICO_POSICOES
        self.publicadas: dict[tuple[str, datetime], datetime] = {}  # (veículo, instante) -> quando publicou
        self.falhas_entrega = 0
        self.gravacao = gzip.open(gravar, "ab") if gravar else None

    def _entregue(self, erro, _mensagem) -> None:
        if erro is not None:
            self.falhas_entrega += 1
            log.error("falha ao publicar: %s", erro)

    def novas(self, posicoes: list[Posicao], agora: datetime) -> list[Posicao]:
        limite = agora - MEMORIA
        self.publicadas = {k: quando for k, quando in self.publicadas.items() if quando >= limite}
        resultado = []
        for p in sorted(posicoes, key=lambda p: p.registrado_em):
            chave = (p.veiculo, p.registrado_em)
            if chave not in self.publicadas:
                self.publicadas[chave] = agora
                resultado.append(p)
        return resultado

    def ciclo(self, agora: datetime | None = None) -> dict[str, int]:
        agora = agora or datetime.now(UTC)
        buscadas = self.fonte.buscar(agora)
        novas = self.novas(buscadas, agora)
        for p in novas:
            valor = p.para_json()
            self.producer.produce(self.topico, key=p.chave, value=valor, on_delivery=self._entregue)
            self.producer.poll(0)  # dispara os callbacks de entrega sem bloquear
            if self.gravacao:
                self.gravacao.write(valor + b"\n")
        pendentes = self.producer.flush(30)
        if self.gravacao:
            self.gravacao.flush()
        return {
            "buscadas": len(buscadas),
            "novas": len(novas),
            "veiculos": len({p.veiculo for p in buscadas}),
            "nao_entregues": pendentes + self.falhas_entrega,
        }

    def fechar(self) -> None:
        self.producer.flush(30)
        if self.gravacao:
            self.gravacao.close()


def rodar(fonte: Fonte, intervalo: float, gravar: Path | None = None, ciclos: int | None = None) -> None:
    kafka.garantir_topicos()
    publicador = Publicador(fonte, Producer(kafka.produtor_config()), gravar=gravar)
    parar = False

    def ao_sinal(*_):
        nonlocal parar
        parar = True

    signal.signal(signal.SIGTERM, ao_sinal)
    signal.signal(signal.SIGINT, ao_sinal)
    feitos, falhas_seguidas = 0, 0
    proximo = time.monotonic()
    while not parar and (ciclos is None or feitos < ciclos):
        try:
            r = publicador.ciclo()
            falhas_seguidas = 0
            log.info(
                "%d posições, %d novas, %d veículos vistos, descartes: %s",
                r["buscadas"], r["novas"], r["veiculos"], dict(fonte.descartes) or "nenhum",
            )
        except Exception as erro:  # noqa: BLE001 - API fora do ar não pode derrubar o produtor
            falhas_seguidas += 1
            log.warning("falha ao buscar (%d seguidas): %s", falhas_seguidas, erro)
        feitos += 1
        # espera o próximo horário; após falhas seguidas, espera mais (até 5 min) para não insistir
        proximo += intervalo * min(2 ** max(0, falhas_seguidas - 1), 10)
        while not parar and time.monotonic() < proximo:
            time.sleep(min(1.0, proximo - time.monotonic()))
    publicador.fechar()
    log.info("produtor encerrado")
