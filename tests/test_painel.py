"""API do painel contra o banco de teste (precisa de DATABASE_URL_TESTE)."""

import os
from datetime import UTC, datetime, timedelta

import pytest
from conftest import trajeto
from fastapi.testclient import TestClient

from onibus import config
from onibus.processador import gravar, inicio_da_janela

pytestmark = pytest.mark.integracao


@pytest.fixture
def cliente(con, monkeypatch):
    monkeypatch.setattr(config, "DATABASE_URL", os.environ["DATABASE_URL_TESTE"])
    from onibus.painel import app

    # dez minutos de dados terminando agora: a janela anterior à atual está completa
    agora = datetime.now(UTC).replace(microsecond=0)
    inicio = inicio_da_janela(agora) - timedelta(minutes=10)
    pontos = (
        trajeto("A1", "474", kmh=30, inicio=inicio)
        + trajeto("A2", "474", kmh=20, inicio=inicio)
        + trajeto("A3", "474", kmh=10, inicio=inicio)
        + trajeto("B1", "309", kmh=6, inicio=inicio)
        + trajeto("B2", "309", kmh=6, inicio=inicio)
        + trajeto("B3", "309", kmh=0, inicio=inicio)
        + trajeto("G1", "GARAGEM", kmh=0, inicio=inicio)
        + trajeto("G2", "GARAGEM", kmh=0, inicio=inicio)
        + trajeto("G3", "GARAGEM", kmh=0, inicio=inicio)
    )
    gravar(con, [p for p in pontos if p.registrado_em <= agora])
    with TestClient(app) as c:
        yield c


def test_resumo_ordena_as_linhas_mais_lentas_e_ignora_garagem(cliente):
    r = cliente.get("/api/resumo").json()
    assert [item["linha"] for item in r["lentas"]] == ["309", "474"]
    assert r["lentas"][0]["parados"] == 1
    assert r["lentas"][1]["velocidade_media"] == pytest.approx(20.0, abs=0.3)
    assert r["agora"]["onibus"] == 6  # os 3 da garagem ficam fora
    assert r["agora"]["linhas"] == 2


def test_onibus_traz_a_ultima_posicao_de_cada_um(cliente):
    r = cliente.get("/api/onibus").json()
    veiculos = {o[r["campos"].index("veiculo")] for o in r["onibus"]}
    assert veiculos == {"A1", "A2", "A3", "B1", "B2", "B3", "G1", "G2", "G3"}


def test_serie_de_uma_linha_e_linha_inexistente(cliente):
    serie = cliente.get("/api/linhas/474").json()["serie"]
    assert serie and all(j["onibus"] == 3 for j in serie)
    assert cliente.get("/api/linhas/999").status_code == 404


def test_saude_resume_os_lotes(cliente):
    r = cliente.get("/api/saude").json()
    assert r["novas"] > 0 and r["duplicadas"] == 0 and r["invalidas"] == 0
    assert r["ultimo_lote_s"] is not None and r["ultimo_lote_s"] < 60


def test_pagina_do_mapa_e_cidade(cliente):
    r = cliente.get("/")
    assert r.status_code == 200 and "maplibre" in r.text
    assert cliente.get("/api/cidade").json()["titulo"] == "Ônibus do Rio, agora"
