"""Linha de comando.

    python -m onibus produtor                    # busca na fonte (FONTE no .env) e publica no Kafka
    python -m onibus produtor --gravar g.jsonl.gz  # também grava o que publicou, para reproduzir depois
    python -m onibus processador                 # consome, grava no PostgreSQL e calcula as janelas
    python -m onibus painel                      # mapa e métricas em http://localhost:8000
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from . import config


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    p = argparse.ArgumentParser(prog="onibus", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="comando", required=True)
    prod = sub.add_parser("produtor")
    prod.add_argument("--fonte", default=config.FONTE, choices=["rio", "sptrans", "gravacao"])
    prod.add_argument("--intervalo", type=float, default=config.INTERVALO_SEGUNDOS, help="segundos entre buscas")
    prod.add_argument("--gravar", type=Path, help="grava as posições publicadas neste arquivo .jsonl.gz")
    prod.add_argument("--ciclos", type=int, help="para depois de N buscas")
    sub.add_parser("processador")
    painel = sub.add_parser("painel")
    painel.add_argument("--porta", type=int, default=8000)
    args = p.parse_args(argv)

    if args.comando == "produtor":
        from . import fontes, produtor

        produtor.rodar(fontes.criar(args.fonte), args.intervalo, gravar=args.gravar, ciclos=args.ciclos)
    elif args.comando == "processador":
        from . import processador

        processador.rodar()
    elif args.comando == "painel":
        import uvicorn

        uvicorn.run("onibus.painel:app", host="0.0.0.0", port=args.porta, log_level="warning")


if __name__ == "__main__":
    main()
