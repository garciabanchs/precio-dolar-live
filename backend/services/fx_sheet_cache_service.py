from __future__ import annotations

import csv
import io
import math
import time
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional


SHEET_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1EW2BU7VBR2g6AyPOQL_P2ok9VD1QMDvRDak2tEGNNC4"
    "/export?format=csv&gid=0"
)

CACHE_TTL_SECONDS = 6 * 60 * 60  # 6 horas

_FX_CACHE: Dict[str, Any] = {
    "loaded_at": 0,
    "rows": None,
}


REFERENCE_COLUMN_MAP = {
    "bcv": "bcv",
    "oficial": "bcv",
    "monitor": "monitor",
    "promedio": "monitor",
    "dolar_promedio": "monitor",
    "binance": "binance",
    "binance_p2p": "binance",
    "usdt": "usdt",
    "bybit": "usdt",
    "bybit_p2p": "usdt",
    "dolartoday": "dolartoday",
    "compuesto": "compuesto",
}


def _normalize_key(value: str) -> str:
    return str(value or "").strip().lower()


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    text = str(value).strip()

    if text == "":
        return None

    text = (
        text.replace("Bs.", "")
        .replace("Bs", "")
        .replace("$", "")
        .replace("%", "")
        .strip()
    )

    # Formato venezolano/español: 1.234,56
    if "," in text:
        text = text.replace(".", "").replace(",", ".")

    try:
        number = float(text)
    except ValueError:
        return None

    if not math.isfinite(number) or number <= 0:
        return None

    return number


def _read_google_sheet_csv() -> List[Dict[str, Any]]:
    with urllib.request.urlopen(SHEET_CSV_URL, timeout=15) as response:
        raw = response.read().decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(raw))
    rows: List[Dict[str, Any]] = []

    for row in reader:
        clean = {}
        for k, v in row.items():
            key = _normalize_key(k)
            clean[key] = v

        if not clean.get("date"):
            continue

        rows.append(clean)

    return rows


def get_fx_history_cached(force_refresh: bool = False) -> List[Dict[str, Any]]:
    now = time.time()

    cache_valid = (
        _FX_CACHE.get("rows") is not None
        and now - float(_FX_CACHE.get("loaded_at", 0)) < CACHE_TTL_SECONDS
    )

    if cache_valid and not force_refresh:
        return _FX_CACHE["rows"]

    rows = _read_google_sheet_csv()

    _FX_CACHE["rows"] = rows
    _FX_CACHE["loaded_at"] = now

    return rows


def _parse_date(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None

    try:
        return datetime.fromisoformat(text[:10])
    except ValueError:
        return None


def _last_valid_row(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    dated_rows = []

    for row in rows:
        d = _parse_date(row.get("date"))
        if d is not None:
            dated_rows.append((d, row))

    if not dated_rows:
        raise ValueError("No hay filas válidas de tipo de cambio en Google Sheet.")

    dated_rows.sort(key=lambda x: x[0])
    return dated_rows[-1][1]


def _row_days_before(rows: List[Dict[str, Any]], current_row: Dict[str, Any], days: int = 7) -> Dict[str, Any]:
    current_date = _parse_date(current_row.get("date"))
    if current_date is None:
        raise ValueError("La última fila válida no tiene fecha válida.")

    candidates = []

    for row in rows:
        d = _parse_date(row.get("date"))
        if d is None:
            continue

        if d < current_date:
            diff = abs((current_date - d).days - days)
            candidates.append((diff, d, row))

    if not candidates:
        raise ValueError("No se encontró fila histórica para t-1 de 7 días.")

    candidates.sort(key=lambda x: (x[0], -x[1].timestamp()))
    return candidates[0][2]


def get_pricing_fx_context(
    selected_reference: str = "compuesto",
    manual_fx_values: Optional[Dict[str, Any]] = None,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    manual_fx_values = manual_fx_values or {}

    rows = get_fx_history_cached(force_refresh=force_refresh)

    today_row = _last_valid_row(rows)
    prev_row = _row_days_before(rows, today_row, days=7)

    selected_key = _normalize_key(selected_reference)
    selected_column = REFERENCE_COLUMN_MAP.get(selected_key, selected_key)

    required_columns = ["bcv", selected_column]

    status: Dict[str, str] = {}
    values_today: Dict[str, Optional[float]] = {}
    values_prev: Dict[str, Optional[float]] = {}
    missing_fields: List[str] = []

    for col in required_columns:
        today_value = _to_float(today_row.get(col))
        prev_value = _to_float(prev_row.get(col))

        manual_value = _to_float(manual_fx_values.get(col))

        if today_value is None and manual_value is not None:
            today_value = manual_value
            status[col] = "manual"
        elif today_value is None:
            status[col] = "missing"
            missing_fields.append(col)
        else:
            status[col] = "auto"

        if prev_value is None:
            # El histórico de 7D no debe completarse manualmente para no contaminar la lógica.
            status[f"{col}_t_1"] = "missing"
            missing_fields.append(f"{col}_t_1")
        else:
            status[f"{col}_t_1"] = "auto"

        values_today[col] = today_value
        values_prev[col] = prev_value

    requires_manual = len(missing_fields) > 0

    fx_factor = None

    tcm_t = values_today.get(selected_column)
    tcbc_t = values_today.get("bcv")
    tcm_t_1 = values_prev.get(selected_column)
    tcbc_t_1 = values_prev.get("bcv")

    if not requires_manual:
        fx_factor = (tcm_t / tcbc_t) / (tcm_t_1 / tcbc_t_1)

    return {
        "source": "google_sheet_fx_data_master",
        "selected_reference": selected_reference,
        "selected_column": selected_column,
        "pricing_period": "7d",
        "today_date": today_row.get("date"),
        "previous_date": prev_row.get("date"),
        "tcm_t": tcm_t,
        "tcbc_t": tcbc_t,
        "tcm_t_1": tcm_t_1,
        "tcbc_t_1": tcbc_t_1,
        "fx_factor": fx_factor,
        "requires_manual": requires_manual,
        "missing_fields": sorted(set(missing_fields)),
        "status": status,
        "today_values": values_today,
        "previous_values": values_prev,
    }
