"""Criação dos tópicos e configurações comuns de produtor e consumidor."""

from __future__ import annotations

import logging

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from . import config

log = logging.getLogger(__name__)

PARTICOES = 3
# Posições valem pouco depois de um dia: o tópico guarda 24 h, o suficiente para reprocessar.
RETENCAO_MS = str(24 * 3600 * 1000)


def produtor_config() -> dict:
    return {
        "bootstrap.servers": config.KAFKA_SERVIDORES,
        "acks": "all",
        "enable.idempotence": True,  # reenvio após falha de rede não duplica no tópico
        "compression.type": "lz4",
        "linger.ms": 50,
    }


def consumidor_config(grupo: str | None = None) -> dict:
    return {
        "bootstrap.servers": config.KAFKA_SERVIDORES,
        "group.id": grupo or config.GRUPO_PROCESSADOR,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # o offset só avança depois que o banco confirmou a gravação
    }


def garantir_topicos(nomes: list[str] | None = None) -> None:
    nomes = nomes or [config.TOPICO_POSICOES, config.TOPICO_INVALIDAS]
    admin = AdminClient({"bootstrap.servers": config.KAFKA_SERVIDORES})
    existentes = set(admin.list_topics(timeout=15).topics)
    novos = [
        NewTopic(n, num_partitions=PARTICOES, replication_factor=1, config={"retention.ms": RETENCAO_MS})
        for n in nomes
        if n not in existentes
    ]
    if not novos:
        return
    for nome, futuro in admin.create_topics(novos).items():
        try:
            futuro.result()
            log.info("tópico criado: %s", nome)
        except KafkaException as erro:  # outro serviço pode ter criado ao mesmo tempo
            if erro.args[0].name() != "TOPIC_ALREADY_EXISTS":
                raise
