"""O evento que circula no Kafka: a posição de um ônibus num instante, já no formato comum.

Cada fonte (Rio, SPTrans, gravação) tem o seu formato; os adaptadores em `fontes/` convertem
para `Posicao`, e daí em diante o pipeline não sabe de onde o dado veio.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

# Retângulos que contêm cada cidade: posição fora disso é GPS com defeito (0,0, troca de sinal...).
AREAS = {
    "rio": (-23.15, -43.85, -22.70, -43.05),  # lat_min, lon_min, lat_max, lon_max
    "sp": (-24.05, -46.85, -23.35, -46.35),
}
ADIANTAMENTO_MAXIMO_S = 120  # relógio do GPS adiantado mais que isso não é confiável


@dataclass(frozen=True)
class Posicao:
    cidade: str
    veiculo: str
    linha: str
    lat: float
    lon: float
    registrado_em: datetime  # instante do GPS, em UTC: é o tempo do evento
    recebido_em: datetime  # quando o produtor buscou na API
    velocidade: float | None = None  # informada pelo veículo, km/h; nem toda fonte tem
    sentido: str | None = None

    @property
    def chave(self) -> bytes:
        """Chave da mensagem: o mesmo veículo cai sempre na mesma partição, em ordem."""
        return f"{self.cidade}:{self.veiculo}".encode()

    def para_json(self) -> bytes:
        dados = asdict(self)
        dados["registrado_em"] = self.registrado_em.isoformat()
        dados["recebido_em"] = self.recebido_em.isoformat()
        return json.dumps(dados, ensure_ascii=False, separators=(",", ":")).encode()

    @classmethod
    def de_json(cls, bruto: bytes) -> Posicao:
        dados = json.loads(bruto)
        dados["registrado_em"] = _utc(datetime.fromisoformat(dados["registrado_em"]))
        dados["recebido_em"] = _utc(datetime.fromisoformat(dados["recebido_em"]))
        p = cls(**dados)
        motivo = problema(p)
        if motivo:
            raise ValueError(motivo)
        return p


def _utc(momento: datetime) -> datetime:
    if momento.tzinfo is None:
        raise ValueError("horário sem fuso")
    return momento.astimezone(UTC)


def problema(p: Posicao) -> str | None:
    """Motivo para descartar a posição, ou None se ela é aproveitável."""
    if not p.veiculo or not p.linha:
        return "sem veículo ou linha"
    area = AREAS.get(p.cidade)
    if area is None:
        return f"cidade desconhecida: {p.cidade}"
    lat_min, lon_min, lat_max, lon_max = area
    if not (lat_min <= p.lat <= lat_max and lon_min <= p.lon <= lon_max):
        return "fora da cidade"
    if (p.registrado_em - p.recebido_em).total_seconds() > ADIANTAMENTO_MAXIMO_S:
        return "horário no futuro"
    return None
