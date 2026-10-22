"""GPS dos ônibus municipais do Rio (API Conecta da Prefeitura). Pública, sem cadastro.

    GET https://dados.mobilidade.rio/sppo/conecta/gps?dataInicial=...Z&dataFinal=...Z

Devolve todos os pontos de GPS que chegaram ao servidor no intervalo pedido (não só o último de
cada ônibus), cerca de um ponto a cada 30 s por veículo:

    {"id_veiculo": "A72100", "servico": "014", "sentido": "V", "latitude": -22.91,
     "longitude": -43.18, "velocidade": 25.0, "datetime": "2026-10-22T09:26:40Z",
     "datetime_envio": "...", "datetime_servidor": "...", "route_id": ..., ...}

Duas pegadinhas, vistas nos dados reais:
- o intervalo pedido é em UTC, mas os horários da resposta estão no horário de Brasília, com
  um "Z" no fim como se fossem UTC. Lidos ao pé da letra, todos os ônibus estariam 3 h no
  futuro;
- o filtro é pelo horário em que o ponto chegou ao servidor, não pelo horário do GPS: chegam
  pontos com vários minutos de atraso, que precisam ir para a janela certa.

Por isso cada busca pede o intervalo desde a busca anterior (com uma sobreposição, para não
perder nada na borda) e o produtor descarta o que já publicou.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta, timezone

import httpx

from ..posicao import Posicao, problema

URL = "https://dados.mobilidade.rio/sppo/conecta/gps"
BRASILIA = timezone(timedelta(hours=-3))  # sem horário de verão desde 2019
SOBREPOSICAO = timedelta(seconds=30)
PRIMEIRA_JANELA = timedelta(minutes=3)
JANELA_MAXIMA = timedelta(minutes=10)  # depois de uma queda longa, não tenta recuperar horas de dados


def _utc(momento: datetime) -> str:
    return momento.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def ler_horario(texto: str) -> datetime:
    """'2026-10-22T09:26:40Z' da API é 09:26:40 em Brasília, ou seja, 12:26:40 UTC."""
    return datetime.fromisoformat(texto.removesuffix("Z")).replace(tzinfo=BRASILIA).astimezone(UTC)


class Rio:
    cidade = "rio"

    def __init__(self, cliente: httpx.Client | None = None):
        self.cliente = cliente or httpx.Client(timeout=60)
        self.ultimo_fim: datetime | None = None
        self.descartes: Counter = Counter()

    def intervalo(self, agora: datetime) -> tuple[datetime, datetime]:
        inicio = self.ultimo_fim - SOBREPOSICAO if self.ultimo_fim else agora - PRIMEIRA_JANELA
        return max(inicio, agora - JANELA_MAXIMA), agora

    def buscar(self, agora: datetime) -> list[Posicao]:
        inicio, fim = self.intervalo(agora)
        resposta = self.cliente.get(URL, params={"dataInicial": _utc(inicio), "dataFinal": _utc(fim)})
        resposta.raise_for_status()
        posicoes = self.converter(resposta.json(), agora)
        self.ultimo_fim = fim  # só avança se a busca deu certo; se falhou, a próxima cobre o buraco
        return posicoes

    def converter(self, dados: list[dict], agora: datetime) -> list[Posicao]:
        posicoes = []
        for r in dados:
            try:
                p = Posicao(
                    cidade=self.cidade,
                    veiculo=str(r["id_veiculo"]),
                    linha=str(r["servico"]),
                    lat=float(r["latitude"]),
                    lon=float(r["longitude"]),
                    registrado_em=ler_horario(r["datetime"]),
                    recebido_em=agora,
                    velocidade=float(r["velocidade"]) if r.get("velocidade") is not None else None,
                    sentido=r.get("sentido") or None,
                )
            except (KeyError, TypeError, ValueError):
                self.descartes["registro incompleto"] += 1
                continue
            motivo = problema(p)
            if motivo:
                self.descartes[motivo] += 1
            else:
                posicoes.append(p)
        return posicoes
