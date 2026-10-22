"""API Olho Vivo da SPTrans (São Paulo). Precisa de um token do portal de desenvolvedores.

Autentica uma vez (a API devolve um cookie de sessão) e busca `/Posicao`, que traz todas as
linhas com os veículos de cada uma:

    {"hr": "11:30", "l": [{"c": "8000-10", "sl": 1, "vs": [{"p": "11433", "ta": "...Z",
                                                          "py": -23.54, "px": -46.64}]}]}
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime

import httpx

from ..posicao import Posicao, problema

BASE = "https://api.olhovivo.sptrans.com.br/v2.1"


class SemToken(RuntimeError):
    pass


class SPTrans:
    cidade = "sp"

    def __init__(self, token: str, cliente: httpx.Client | None = None):
        if not token:
            raise SemToken("Defina SPTRANS_TOKEN no .env (crie em sptrans.com.br/desenvolvedores).")
        self.token = token
        self.cliente = cliente or httpx.Client(timeout=30)
        self.autenticado = False
        self.descartes: Counter = Counter()

    def _autenticar(self) -> None:
        resposta = self.cliente.post(f"{BASE}/Login/Autenticar", params={"token": self.token})
        resposta.raise_for_status()
        if resposta.json() is not True:
            raise SemToken("A SPTrans recusou o token. Confira o SPTRANS_TOKEN no .env.")
        self.autenticado = True

    def buscar(self, agora: datetime) -> list[Posicao]:
        if not self.autenticado:
            self._autenticar()
        resposta = self.cliente.get(f"{BASE}/Posicao")
        if resposta.status_code == 401:  # sessão expirou: autentica de novo uma vez
            self._autenticar()
            resposta = self.cliente.get(f"{BASE}/Posicao")
        resposta.raise_for_status()
        return self.converter(resposta.json(), agora)

    def converter(self, dados: dict, agora: datetime) -> list[Posicao]:
        posicoes = []
        for linha in dados.get("l") or []:
            for v in linha.get("vs") or []:
                try:
                    p = Posicao(
                        cidade=self.cidade,
                        veiculo=str(v["p"]),
                        linha=str(linha["c"]),
                        lat=float(v["py"]),
                        lon=float(v["px"]),
                        registrado_em=datetime.fromisoformat(v["ta"]).astimezone(UTC),
                        recebido_em=agora,
                        sentido=str(linha.get("sl")) if linha.get("sl") is not None else None,
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
