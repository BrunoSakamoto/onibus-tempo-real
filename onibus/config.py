"""Configuração por variáveis de ambiente (as mesmas no Docker Compose e rodando local)."""

from __future__ import annotations

import os
from pathlib import Path


def _env(nome: str, padrao: str) -> str:
    return os.getenv(nome) or padrao


def carregar_dotenv(caminho: Path = Path(".env")) -> None:
    """Lê um .env simples (CHAVE=valor) sem sobrescrever o que já está no ambiente."""
    if not caminho.exists():
        return
    for linha in caminho.read_text(encoding="utf-8").splitlines():
        linha = linha.strip()
        if linha and not linha.startswith("#") and "=" in linha:
            chave, valor = linha.split("=", 1)
            os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))


carregar_dotenv()

KAFKA_SERVIDORES = _env("KAFKA_SERVIDORES", "localhost:9092")
TOPICO_POSICOES = _env("TOPICO_POSICOES", "posicoes")
TOPICO_INVALIDAS = _env("TOPICO_INVALIDAS", "posicoes-invalidas")
GRUPO_PROCESSADOR = _env("GRUPO_PROCESSADOR", "processador")
DATABASE_URL = _env("DATABASE_URL", "postgresql://onibus:onibus@localhost:5432/onibus")

FONTE = _env("FONTE", "rio")  # rio | sptrans | gravacao
CIDADE = _env("CIDADE", "sp" if FONTE == "sptrans" else "rio")  # a cidade que o painel mostra
INTERVALO_SEGUNDOS = float(_env("INTERVALO_SEGUNDOS", "30"))
SPTRANS_TOKEN = os.getenv("SPTRANS_TOKEN", "")
GRAVACAO = Path(_env("GRAVACAO", "gravacoes/amostra-rio.jsonl.gz"))

JANELA_MINUTOS = 5
RETENCAO_HORAS = int(_env("RETENCAO_HORAS", "6"))
