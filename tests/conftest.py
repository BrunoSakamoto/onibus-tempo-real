from __future__ import annotations

import math
import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from onibus import config
from onibus.posicao import Posicao

CENTRO_RIO = (-22.9068, -43.1729)
T0 = datetime(2026, 10, 22, 12, 0, tzinfo=UTC)


def trajeto(
    veiculo: str,
    linha: str = "474",
    kmh: float = 30.0,
    inicio: datetime = T0,
    pontos: int = 21,
    passo_s: int = 30,
    origem: tuple[float, float] = CENTRO_RIO,
) -> list[Posicao]:
    """Um ônibus andando para o norte em velocidade constante, com um ponto a cada `passo_s`."""
    metros_por_grau = 2 * math.pi * 6371000 / 360
    resultado = []
    for i in range(pontos):
        momento = inicio + timedelta(seconds=i * passo_s)
        lat = origem[0] + (kmh / 3.6) * i * passo_s / metros_por_grau
        resultado.append(Posicao(
            cidade="rio", veiculo=veiculo, linha=linha, lat=lat, lon=origem[1],
            registrado_em=momento, recebido_em=momento + timedelta(seconds=20),
        ))
    return resultado


@pytest.fixture
def con():
    """Banco de teste limpo. Pula se não houver PostgreSQL (rode com DATABASE_URL_TESTE)."""
    url = os.getenv("DATABASE_URL_TESTE")
    if not url:
        pytest.skip("defina DATABASE_URL_TESTE para os testes com PostgreSQL")
    from onibus import banco

    conexao = banco.conectar(url)
    conexao.execute("drop table if exists posicoes, posicao_atual, janela_linha, lotes cascade")
    banco.criar_esquema(conexao)
    yield conexao
    conexao.close()


@pytest.fixture
def kafka_isolado(monkeypatch):
    """Tópicos e grupo com nomes únicos por teste. Pula se não houver Kafka (KAFKA_SERVIDORES)."""
    if not os.getenv("KAFKA_SERVIDORES"):
        pytest.skip("defina KAFKA_SERVIDORES para os testes com Kafka")
    sufixo = uuid.uuid4().hex[:8]
    monkeypatch.setattr(config, "KAFKA_SERVIDORES", os.environ["KAFKA_SERVIDORES"])
    monkeypatch.setattr(config, "TOPICO_POSICOES", f"posicoes-{sufixo}")
    monkeypatch.setattr(config, "TOPICO_INVALIDAS", f"invalidas-{sufixo}")
    monkeypatch.setattr(config, "GRUPO_PROCESSADOR", f"grupo-{sufixo}")
    return sufixo
