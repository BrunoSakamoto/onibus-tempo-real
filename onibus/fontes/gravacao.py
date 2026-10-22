"""Reproduz uma gravação feita pelo produtor (`--gravar`), como se fosse a API ao vivo.

Serve para rodar o projeto sem internet, na demonstração e nos testes. A gravação é uma
sequência de ciclos (cada busca na API); cada `buscar` devolve o próximo ciclo trazido para o
presente: o ciclo passa a ter sido recebido agora, e cada posição mantém o atraso que tinha em
relação à busca. Ao chegar ao fim, recomeça do primeiro ciclo. Rode com o mesmo intervalo da
gravação (30 s): com buscas mais rápidas, o mesmo deslocamento caberia em menos tempo e as
velocidades sairiam multiplicadas.
"""

from __future__ import annotations

import gzip
from collections import Counter
from dataclasses import replace
from datetime import datetime
from itertools import groupby
from pathlib import Path

from ..posicao import Posicao


class Gravacao:
    def __init__(self, caminho: Path):
        if not caminho.exists():
            raise FileNotFoundError(f"Gravação não encontrada: {caminho}")
        abrir = gzip.open if caminho.suffix == ".gz" else open
        with abrir(caminho, "rb") as arquivo:
            posicoes = [Posicao.de_json(linha) for linha in arquivo if linha.strip()]
        if not posicoes:
            raise ValueError(f"Gravação vazia: {caminho}")
        self.ciclos = [list(g) for _, g in groupby(posicoes, key=lambda p: p.recebido_em)]
        self.cidade = posicoes[0].cidade
        self.proximo = 0
        self.descartes: Counter = Counter()

    def buscar(self, agora: datetime) -> list[Posicao]:
        ciclo = self.ciclos[self.proximo % len(self.ciclos)]
        self.proximo += 1
        deslocar = agora - ciclo[0].recebido_em
        return [
            replace(p, registrado_em=p.registrado_em + deslocar, recebido_em=agora) for p in ciclo
        ]
