from copy import deepcopy

EXPECTED_SOURCES = [
    "bcv",
    "monitor",
    "promedio",
    "binance",
    "usdt",
    "dolartoday",
]

SOURCE_LABELS = {
    "bcv": "BCV",
    "monitor": "Monitor",
    "promedio": "Promedio",
    "binance": "Binance",
    "usdt": "USDT",
    "dolartoday": "DolarToday",
}


def _to_float(value):
    if value is None:
        return None
    try:
        value = float(value)
        return value
    except Exception:
        return None


def _normalize_partial_sources(raw_sources: dict) -> dict:
    """
    Convierte cualquier entrada parcial a un formato estándar:
    {
      "bcv": {"label":"BCV","value":482.7,"status":"auto|missing|manual"}
    }
    """
    normalized = {}

    for key in EXPECTED_SOURCES:
        item = raw_sources.get(key, {})
        value = _to_float(item.get("value")) if isinstance(item, dict) else None
        status = item.get("status") if isinstance(item, dict) else None

        if value is None:
            normalized[key] = {
                "label": SOURCE_LABELS[key],
                "value": None,
                "status": "missing"
            }
        else:
            normalized[key] = {
                "label": SOURCE_LABELS[key],
                "value": value,
                "status": status if status in ["auto", "manual"] else "auto"
            }

    return normalized


def build_fx_check_response(raw_sources: dict, as_of_date: str = None) -> dict:
    """
    Construye la respuesta de /fx/check
    """
    sources = _normalize_partial_sources(raw_sources)

    missing_sources = [
        key for key, item in sources.items()
        if item["value"] is None
    ]

    missing_count = len(missing_sources)
    all_complete = missing_count == 0

    if all_complete:
        message = "Se obtuvieron automáticamente los 6 tipos de cambio. Puede continuar."
    else:
        missing_labels = [SOURCE_LABELS[key] for key in missing_sources]
        message = (
            f"Se obtuvieron automáticamente {len(EXPECTED_SOURCES) - missing_count} de {len(EXPECTED_SOURCES)} "
            f"tipos de cambio. Debe completar manualmente: {', '.join(missing_labels)}."
        )

    return {
        "success": True,
        "all_complete": all_complete,
        "missing_count": missing_count,
        "missing_sources": missing_sources,
        "sources": sources,
        "as_of_date": as_of_date,
        "message": message
    }


def validate_manual_completion(auto_sources: dict, manual_sources: dict) -> dict:
    """
    Fusiona lo automático con lo manual y exige que todos los faltantes queden completos.
    manual_sources esperado:
    {
      "usdt": 622.80,
      "dolartoday": 625.30
    }
    """
    merged = deepcopy(_normalize_partial_sources(auto_sources))

    manual_sources = manual_sources or {}

    # Solo se intentan completar los missing
    for key, value in manual_sources.items():
        if key not in EXPECTED_SOURCES:
            raise ValueError(f"Fuente manual no soportada: {key}")

        numeric_value = _to_float(value)

        if numeric_value is None:
            raise ValueError(f"El valor manual para {SOURCE_LABELS[key]} debe ser numérico.")

        if numeric_value <= 0:
            raise ValueError(f"El valor manual para {SOURCE_LABELS[key]} debe ser mayor que 0.")

        merged[key] = {
            "label": SOURCE_LABELS[key],
            "value": numeric_value,
            "status": "manual"
        }

    # Validación final
    missing_sources = [
        key for key, item in merged.items()
        if item["value"] is None
    ]

    if missing_sources:
        missing_labels = [SOURCE_LABELS[key] for key in missing_sources]
        raise ValueError(
            f"Aún faltan tipos de cambio por completar: {', '.join(missing_labels)}."
        )

    return merged


def build_fx_complete_response(merged_sources: dict, as_of_date: str = None) -> dict:
    """
    Construye la respuesta de /fx/complete
    """
    sources = _normalize_partial_sources(merged_sources)

    missing_sources = [
        key for key, item in sources.items()
        if item["value"] is None
    ]

    missing_count = len(missing_sources)
    all_complete = missing_count == 0

    if not all_complete:
        missing_labels = [SOURCE_LABELS[key] for key in missing_sources]
        raise ValueError(
            f"No se puede continuar. Siguen faltando: {', '.join(missing_labels)}."
        )

    return {
        "success": True,
        "all_complete": True,
        "missing_count": 0,
        "missing_sources": [],
        "sources": sources,
        "as_of_date": as_of_date,
        "message": "Todos los tipos de cambio quedaron completos. Puede continuar."
    }


def to_pricing_snapshot(sources: dict) -> dict:
    """
    Devuelve snapshot plano para usar en pricing y frontend.
    """
    normalized = _normalize_partial_sources(sources)

    missing_sources = [
        key for key, item in normalized.items()
        if item["value"] is None
    ]

    if missing_sources:
        missing_labels = [SOURCE_LABELS[key] for key in missing_sources]
        raise ValueError(
            f"No se puede construir snapshot de pricing. Faltan: {', '.join(missing_labels)}."
        )

    return {
        key: normalized[key]["value"]
        for key in EXPECTED_SOURCES
    }