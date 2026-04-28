import requests
from datetime import datetime
from typing import Dict, List, Optional


BASE_URL = "https://ve.dolarapi.com/v1"


def _get_json(url: str):
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    return response.json()


def _to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _normalize_source_key(source_key: str) -> str:
    """
    Compatibilidad temporal con tu frontend/lógica actual.
    """
    source_key = (source_key or "").strip().lower()

    mapping = {
        "bcv": "oficial",
        "oficial": "oficial",
        "monitor": "paralelo",
        "promedio": "paralelo",
        "paralelo": "paralelo",
        "binance": "paralelo",
        "usdt": "paralelo",
        "dolartoday": "paralelo",
    }

    if source_key not in mapping:
        raise ValueError(f"Fuente no soportada: {source_key}")

    return mapping[source_key]


def _days_for_period(period_key: str) -> int:
    mapping = {
        "d7": 7,
        "m1": 30,
        "m3": 90,
        "y1": 365,
    }
    if period_key not in mapping:
        raise ValueError(f"Periodo no soportado: {period_key}")
    return mapping[period_key]


def _pick_current_rate(item: Dict) -> float:
    """
    DolarApi devuelve compra/venta/promedio.
    Usamos promedio si existe; si no, venta; si no, compra.
    """
    for key in ["promedio", "venta", "compra"]:
        value = _to_float(item.get(key))
        if value is not None:
            return value
    raise ValueError("No se encontró tasa numérica en respuesta actual.")


def _pick_historical_rate(item: Dict) -> float:
    """
    Históricos: mismo criterio.
    """
    for key in ["promedio", "venta", "compra"]:
        value = _to_float(item.get(key))
        if value is not None:
            return value
    raise ValueError("No se encontró tasa numérica en respuesta histórica.")


def _find_historical_source_value(rows: List[Dict], fuente_objetivo: str, days_back: int) -> Optional[Dict]:
    """
    Busca la primera observación de la fuente pedida con al menos `days_back`
    días de distancia respecto al registro más reciente disponible.
    """
    if not rows:
        return None

    parsed = []
    for row in rows:
        fecha = row.get("fecha")
        fuente = (row.get("fuente") or "").strip().lower()
        try:
            dt = datetime.fromisoformat(fecha).date()
        except Exception:
            continue
        parsed.append({
            "fecha": dt,
            "fuente": fuente,
            "row": row
        })

    if not parsed:
        return None

    max_date = max(x["fecha"] for x in parsed)
    target_delta = days_back

    candidates = [
        x for x in parsed
        if x["fuente"] == fuente_objetivo and (max_date - x["fecha"]).days >= target_delta
    ]

    if not candidates:
        # fallback: la más antigua disponible de esa fuente
        fallback = [x for x in parsed if x["fuente"] == fuente_objetivo]
        if not fallback:
            return None
        return sorted(fallback, key=lambda x: x["fecha"])[0]["row"]

    # la fecha más cercana por debajo o igual al corte
    candidates = sorted(
        candidates,
        key=lambda x: ((max_date - x["fecha"]).days - target_delta, x["fecha"])
    )
    return candidates[0]["row"]


def get_real_fx_snapshot():
    """
    Snapshot real con DolarApi.
    """
    current_official = _get_json(f"{BASE_URL}/dolares/oficial")
    current_parallel = _get_json(f"{BASE_URL}/dolares/paralelo")
    historical_official = _get_json(f"{BASE_URL}/historicos/dolares/oficial")
    historical_parallel = _get_json(f"{BASE_URL}/historicos/dolares/paralelo")

    # Asumimos que la fecha actual útil es la del oficial actual si viene; si no, hoy.
    as_of_date = (
        current_official.get("fechaActualizacion")
        or current_parallel.get("fechaActualizacion")
        or datetime.today().strftime("%Y-%m-%d")
    )

    snapshot = {
        "as_of_date": as_of_date,
        "sources": {
            "oficial": {
                "label": "BCV / Oficial",
                "today": _pick_current_rate(current_official),
            },
            "paralelo": {
                "label": "Paralelo",
                "today": _pick_current_rate(current_parallel),
            },
        },
        "historical": {
            "oficial": historical_official,
            "paralelo": historical_parallel,
        }
    }

    return snapshot


def get_reference_pair(snapshot: dict, market_source: str = "paralelo", period_key: str = "d7"):
    """
    Devuelve:
    - TCM_t
    - TCBCV_t
    - TCM_t-1
    - TCBCV_t-1
    """
    normalized_source = _normalize_source_key(market_source)
    days_back = _days_for_period(period_key)

    tcm_t = snapshot["sources"][normalized_source]["today"]
    tcbc_t = snapshot["sources"]["oficial"]["today"]

    historical_market = _find_historical_source_value(
        snapshot["historical"][normalized_source],
        normalized_source,
        days_back
    )
    historical_official = _find_historical_source_value(
        snapshot["historical"]["oficial"],
        "oficial",
        days_back
    )

    if not historical_market:
        raise ValueError(f"No se consiguió histórico para la fuente {normalized_source}.")
    if not historical_official:
        raise ValueError("No se consiguió histórico para la tasa oficial.")

    tcm_t_1 = _pick_historical_rate(historical_market)
    tcbc_t_1 = _pick_historical_rate(historical_official)

    return {
        "market_source_requested": market_source,
        "market_source_effective": normalized_source,
        "period_key": period_key,
        "days_back": days_back,
        "tcm_t": tcm_t,
        "tcbc_t": tcbc_t,
        "tcm_t_1": tcm_t_1,
        "tcbc_t_1": tcbc_t_1,
        "market_historical_date": historical_market.get("fecha"),
        "official_historical_date": historical_official.get("fecha"),
    }


def get_fx_summary(snapshot: dict):
    return [
        {
            "key": "oficial",
            "label": snapshot["sources"]["oficial"]["label"],
            "today": snapshot["sources"]["oficial"]["today"],
        },
        {
            "key": "paralelo",
            "label": snapshot["sources"]["paralelo"]["label"],
            "today": snapshot["sources"]["paralelo"]["today"],
        },
    ]

def get_partial_real_fx_sources():
    """
    Fase actual:
    - BCV / oficial real
    - paralelo real
    - el resto queda pendiente hasta conectar más fuentes reales
    """
    snapshot = get_real_fx_snapshot()

    return {
        "as_of_date": snapshot["as_of_date"],
        "sources": {
            "bcv": {
                "label": "BCV",
                "value": snapshot["sources"]["oficial"]["today"],
                "status": "auto"
            },
            "monitor": {
                "label": "Monitor",
                "value": snapshot["sources"]["paralelo"]["today"],
                "status": "auto"
            },
            "promedio": {
                "label": "Promedio",
                "value": None,
                "status": "missing"
            },
            "binance": {
                "label": "Binance",
                "value": None,
                "status": "missing"
            },
            "usdt": {
                "label": "USDT",
                "value": None,
                "status": "missing"
            },
            "dolartoday": {
                "label": "DolarToday",
                "value": None,
                "status": "missing"
            }
        }
    }