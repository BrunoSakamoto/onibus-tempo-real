"""Fontes de posições. Cada uma sabe buscar no seu formato e converter para `Posicao`."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Protocol

from ..posicao import Posicao


class Fonte(Protocol):
    cidade: str
    descartes: Counter  # motivo -> quantidade, acumulado desde o início

    def buscar(self, agora: datetime) -> list[Posicao]: ...


def criar(nome: str) -> Fonte:
    from .. import config

    if nome == "rio":
        from .rio import Rio

        return Rio()
    if nome == "sptrans":
        from .sptrans import SPTrans

        return SPTrans(config.SPTRANS_TOKEN)
    if nome == "gravacao":
        from .gravacao import Gravacao

        return Gravacao(config.GRAVACAO)
    raise ValueError(f"Fonte desconhecida: {nome} (use rio, sptrans ou gravacao)")
