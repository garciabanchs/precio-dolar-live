import pandas as pd
from io import BytesIO


def _find_sheet(sheet_names, keyword):
    for sheet in sheet_names:
        if keyword.lower() in sheet.lower():
            return sheet
    return None


def _safe_str(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def _safe_float(value):
    if pd.isna(value) or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _normalize_sku(value):
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def _normalize_date(value):
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return ""
        return str(dt.date())
    except Exception:
        return ""


def _normalize_yes_no(value):
    text = _safe_str(value).lower()
    return text in ["sí", "si", "true", "1", "x"]


def _read_empresa_from_contact_sheet(excel_data):
    try:
        raw = pd.read_excel(excel_data, sheet_name="Cómo contactarme", header=None)
        return _safe_str(raw.iloc[6, 2])  # C7
    except Exception:
        return ""


def _parse_market_sheet(excel_data, sheet_name):
    raw = pd.read_excel(excel_data, sheet_name=sheet_name, header=None)

    # Metadatos fijos según tu diseño actual
    ciudad = _safe_str(raw.iloc[6, 0]) if len(raw) > 6 else ""
    region = _safe_str(raw.iloc[8, 0]) if len(raw) > 8 else ""

    # Encabezados principales en fila Excel 11 => índice 10
    df = pd.read_excel(excel_data, sheet_name=sheet_name, header=10)
    df = df.dropna(how="all").reset_index(drop=True)

    if len(df) < 2:
        return None

    # Fila 0: labels de pesos / fila 1: valores
    pesos_values_row = df.iloc[1].copy()

    peso_lider = _safe_float(pesos_values_row.iloc[6] if len(pesos_values_row) > 6 else None)
    peso_intermedio = _safe_float(pesos_values_row.iloc[7] if len(pesos_values_row) > 7 else None)
    peso_economico = _safe_float(pesos_values_row.iloc[8] if len(pesos_values_row) > 8 else None)

    # Cuerpo real: desde la fila 2 en adelante
    df = df.iloc[2:].copy().reset_index(drop=True)
    df = df.dropna(how="all").reset_index(drop=True)

    # Renombrar columnas a nombres cortos
    df = df.rename(columns={
        "Nombre del producto": "nombre_producto",
        "Código del producto (SKU, código de barras, EAN-UPC)": "sku",
        "Unidad o presentación": "unidad_presentacion",
        "Fecha (dd-mm-aaaa)": "fecha",
        "¿Se vende en este mercado?": "se_vende",
        "Precio propio (USD con IVA)": "precio_propio_usd",
        "Precio competencia líder (USD con IVA)": "precio_lider_usd",
        "Precio competencia intermedio (USD con IVA)": "precio_intermedio_usd",
        "Precio competencia económico (USD con IVA)": "precio_economico_usd",
        "Precio promedio de la competencia (USD con IVA)": "precio_promedio_competencia_usd",
        "Peso de la Preocupación por la competencia, cuota de mercado, rotación y flujo": "peso_competencia",
        "Peso de la Preocupación por el riesgo cambiario y la descapitalización": "peso_riesgo_cambiario",
    }).copy()

    # Normalización base
    df["nombre_producto"] = df["nombre_producto"].apply(_safe_str)
    df["sku"] = df["sku"].apply(_normalize_sku)
    df["unidad_presentacion"] = df["unidad_presentacion"].apply(_safe_str)
    df["fecha"] = df["fecha"].apply(_normalize_date)
    df["se_vende"] = df["se_vende"].apply(_normalize_yes_no)

    numeric_cols = [
        "precio_propio_usd",
        "precio_lider_usd",
        "precio_intermedio_usd",
        "precio_economico_usd",
        "precio_promedio_competencia_usd",
        "peso_competencia",
        "peso_riesgo_cambiario",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Quedarse con filas que realmente tienen producto
    df = df[df["nombre_producto"].astype(str).str.strip() != ""].copy()
    df = df.reset_index(drop=True)

    # Si el mercado está vacío, ignorarlo
    if df.empty:
        return None

    # Si los pesos por producto no vienen, los dejamos en NaN.
    # El motor decidirá si se fuerzan a 0/1 según exista o no competencia.
    df["peso_competencia"] = pd.to_numeric(df["peso_competencia"], errors="coerce")
    df["peso_riesgo_cambiario"] = pd.to_numeric(df["peso_riesgo_cambiario"], errors="coerce")

    return {
        "sheet_name": sheet_name,
        "ciudad": ciudad,
        "region": region,
        "peso_lider": peso_lider,
        "peso_intermedio": peso_intermedio,
        "peso_economico": peso_economico,
        "productos": df,
    }


def read_excel_payload(file_bytes: bytes) -> dict:
    excel_data = pd.ExcelFile(BytesIO(file_bytes))
    sheet_names = excel_data.sheet_names

    mercado1_sheet = _find_sheet(sheet_names, "mercado 1")
    mercado2_sheet = _find_sheet(sheet_names, "mercado 2")
    mercado3_sheet = _find_sheet(sheet_names, "mercado 3")

    if not mercado1_sheet:
        raise ValueError("No se encontró la hoja de Mercado 1.")

    empresa = _read_empresa_from_contact_sheet(excel_data)

    mercados = []

    for sheet in [mercado1_sheet, mercado2_sheet, mercado3_sheet]:
        if not sheet:
            continue
        parsed = _parse_market_sheet(excel_data, sheet)
        if parsed is not None:
            mercados.append(parsed)

    return {
        "empresa": empresa,
        "mercados": mercados
    }