"""API do painel (FastAPI) e a página do mapa. Só lê do PostgreSQL; quem escreve é o processador."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from . import config

ESTATICO = Path(__file__).parent / "estatico"
pool: ConnectionPool | None = None


@asynccontextmanager
async def ciclo_de_vida(_app: FastAPI):
    global pool
    pool = ConnectionPool(config.DATABASE_URL, min_size=1, max_size=4, kwargs={"row_factory": dict_row}, open=True)
    yield
    pool.close()


app = FastAPI(title="Ônibus em tempo real", lifespan=ciclo_de_vida)


def consultar(sql: str, parametros: dict | None = None) -> list[dict]:
    assert pool is not None
    with pool.connection() as con:
        return con.execute(sql, parametros or {}).fetchall()


# A janela "agora" é a última já fechada: a que está aberta ainda tem poucos pontos.
# Ônibus em GARAGEM, MANUTENCAO ou ESPECIAL não estão em linha: ficam no mapa, fora das médias.
EM_SERVICO = "linha ~ '[0-9]'"
ULTIMA_JANELA_FECHADA = "date_bin('5 minutes', now(), timestamptz '2000-01-01') - interval '5 minutes'"


CIDADES = {
    "rio": {"nome": "Rio", "titulo": "Ônibus do Rio, agora", "fonte": "API de GPS da Prefeitura do Rio",
            "limites": [[-43.80, -23.08], [-43.05, -22.74]]},
    "sp": {"nome": "São Paulo", "titulo": "Ônibus de São Paulo, agora", "fonte": "API Olho Vivo da SPTrans",
           "limites": [[-46.83, -23.80], [-46.36, -23.40]]},
}


@app.get("/api/cidade")
def cidade_atual() -> dict:
    return {"id": config.CIDADE, **CIDADES[config.CIDADE]}


@app.get("/api/onibus")
def onibus(cidade: str | None = None) -> dict:
    """Última posição de cada ônibus visto nos últimos 5 minutos, em listas compactas."""
    linhas = consultar(
        """
        select veiculo, linha, round(lat::numeric, 5)::float8 as lat, round(lon::numeric, 5)::float8 as lon,
               velocidade, extract(epoch from now() - registrado_em)::int as idade_s
        from posicao_atual
        where cidade = %(cidade)s and registrado_em > now() - interval '5 minutes'
        """,
        {"cidade": cidade or config.CIDADE},
    )
    return {"campos": ["veiculo", "linha", "lat", "lon", "velocidade", "idade_s"],
            "onibus": [list(r.values()) for r in linhas]}


@app.get("/api/resumo")
def resumo(cidade: str | None = None) -> dict:
    """Números da última janela fechada, as linhas mais lentas e a série da cidade nas últimas 3 h."""
    agora = consultar(
        f"""
        select inicio, sum(onibus)::int as onibus, sum(parados)::int as parados,
               round((sum(velocidade_media * onibus) / nullif(sum(onibus) filter (where velocidade_media
                     is not null), 0))::numeric, 1)::float8 as velocidade_media,
               count(*)::int as linhas
        from janela_linha
        where cidade = %(cidade)s and inicio = {ULTIMA_JANELA_FECHADA} and {EM_SERVICO}
        group by inicio
        """,
        {"cidade": cidade or config.CIDADE},
    )
    lentas = consultar(
        f"""
        select linha, onibus, parados, velocidade_media
        from janela_linha
        where cidade = %(cidade)s and inicio = {ULTIMA_JANELA_FECHADA} and {EM_SERVICO}
          and onibus >= 3 and velocidade_media is not null
        order by velocidade_media
        limit 12
        """,
        {"cidade": cidade or config.CIDADE},
    )
    # Janelas com bem menos ônibus que o normal são o começo da coleta (só pontos atrasados
    # chegaram até ali) e dariam uma média de meia dúzia de ônibus: ficam fora da série.
    serie = consultar(
        f"""
        select inicio, onibus, velocidade_media from (
            select inicio, sum(onibus)::int as onibus,
                   round((sum(velocidade_media * onibus) / nullif(sum(onibus), 0))::numeric, 1)::float8
                       as velocidade_media,
                   max(sum(onibus)) over () as maximo
            from janela_linha
            where cidade = %(cidade)s and velocidade_media is not null and {EM_SERVICO}
              and inicio >= now() - interval '3 hours'
              and inicio < date_bin('5 minutes', now(), timestamptz '2000-01-01')
            group by inicio
        ) j
        where onibus >= maximo / 2
        order by inicio
        """,
        {"cidade": cidade or config.CIDADE},
    )
    return {"agora": agora[0] if agora else None, "lentas": lentas, "serie": serie}


@app.get("/api/linhas/{linha}")
def linha(linha: str, cidade: str | None = None) -> dict:
    serie = consultar(
        """
        select inicio, onibus, parados, velocidade_media
        from janela_linha
        where cidade = %(cidade)s and linha = %(linha)s and inicio >= now() - interval '3 hours'
          and inicio < date_bin('5 minutes', now(), timestamptz '2000-01-01')
        order by inicio
        """,
        {"cidade": cidade or config.CIDADE, "linha": linha},
    )
    if not serie:
        raise HTTPException(404, f"Linha {linha} sem dados nas últimas 3 horas")
    return {"linha": linha, "serie": serie}


@app.get("/api/saude")
def saude() -> dict:
    """Como o pipeline está nos últimos 10 minutos."""
    linhas = consultar(
        """
        select coalesce(sum(mensagens), 0)::int as mensagens,
               coalesce(sum(novas), 0)::int as novas,
               coalesce(sum(duplicadas), 0)::int as duplicadas,
               coalesce(sum(invalidas), 0)::int as invalidas,
               round(percentile_cont(0.5) within group (order by atraso_fonte_s)::numeric, 1)::float8
                   as atraso_fonte_s,
               round(percentile_cont(0.5) within group (order by atraso_pipeline_s)::numeric, 1)::float8
                   as atraso_pipeline_s,
               extract(epoch from now() - max(instante))::int as ultimo_lote_s
        from lotes where instante > now() - interval '10 minutes'
        """
    )
    return linhas[0]


@app.get("/")
def pagina() -> FileResponse:
    return FileResponse(ESTATICO / "index.html")
