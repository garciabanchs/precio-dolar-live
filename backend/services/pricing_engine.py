import pandas as pd


def safe_div(numerator, denominator, default=1.0):
    if denominator in [0, None] or pd.isna(denominator):
        return default
    if numerator is None or pd.isna(numerator):
        return default
    return numerator / denominator


def compute_fx_factor(tcm_t, tcbc_t, tcm_t_1, tcbc_t_1):
    """
    Calcula:
    ((TCM_t / TCBCV_t) / (TCM_t-1 / TCBCV_t-1))
    """
    current_gap = safe_div(tcm_t, tcbc_t, default=1.0)
    previous_gap = safe_div(tcm_t_1, tcbc_t_1, default=1.0)
    return safe_div(current_gap, previous_gap, default=1.0)


def _to_float(value):
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _compute_competitor_average(row, mercado):
    """
    Calcula el promedio ponderado de competencia usando solo los precios presentes.
    Reglas:
    - Si no hay ningún precio de competencia -> devuelve None
    - Si hay 1, 2 o 3 precios, los pesos correspondientes deben sumar 1
    - Los pesos vienen a nivel mercado: líder/intermedio/económico
    """
    competitors = []

    precio_lider = _to_float(row.get("precio_lider_usd"))
    precio_intermedio = _to_float(row.get("precio_intermedio_usd"))
    precio_economico = _to_float(row.get("precio_economico_usd"))

    peso_lider = _to_float(mercado.get("peso_lider"))
    peso_intermedio = _to_float(mercado.get("peso_intermedio"))
    peso_economico = _to_float(mercado.get("peso_economico"))

    if precio_lider is not None:
        competitors.append(("lider", precio_lider, peso_lider))
    if precio_intermedio is not None:
        competitors.append(("intermedio", precio_intermedio, peso_intermedio))
    if precio_economico is not None:
        competitors.append(("economico", precio_economico, peso_economico))

    if len(competitors) == 0:
        return None

    # Todos los competidores presentes deben tener peso válido
    for _, _, peso in competitors:
        if peso is None or peso < 0:
            raise ValueError("Faltan pesos válidos de competidores para calcular el promedio de competencia.")

    total_weight = sum([peso for _, _, peso in competitors])

    if round(total_weight, 6) != 1.0:
        raise ValueError("La suma de los pesos de competidores presentes debe ser 1.")

    promedio = sum([precio * peso for _, precio, peso in competitors])
    return promedio


def _resolve_weights_and_competition(row, mercado):
    """
    Reglas de negocio:
    1) precio_propio_usd es obligatorio
    2) si no hay ningún precio de competencia:
       - promedio_competencia = precio_propio
       - peso_competencia = 0
       - peso_riesgo_cambiario = 1
    3) si sí hay competencia:
       - se recalcula promedio_competencia con los precios presentes y sus pesos
       - se usan los pesos del producto si existen
       - si faltan, se intenta normalizar
       - si no existen ambos, se fuerza alpha=0 beta=1 como fallback conservador
    """
    precio_propio = _to_float(row.get("precio_propio_usd"))
    if precio_propio is None or precio_propio <= 0:
        return {
            "producto_valido": False,
            "motivo": "Producto sin precio propio válido"
        }

    promedio_competencia = _compute_competitor_average(row, mercado)

    if promedio_competencia is None:
        return {
            "producto_valido": True,
            "precio_propio_usd": precio_propio,
            "precio_promedio_competencia_usd_final": precio_propio,
            "peso_competencia_final": 0.0,
            "peso_riesgo_cambiario_final": 1.0,
            "competencia_disponible": False
        }

    alpha = _to_float(row.get("peso_competencia"))
    beta = _to_float(row.get("peso_riesgo_cambiario"))

    if alpha is None and beta is None:
        alpha = 0.0
        beta = 1.0
    elif alpha is None and beta is not None:
        alpha = 1.0 - beta
    elif beta is None and alpha is not None:
        beta = 1.0 - alpha

    if alpha is None or beta is None:
        alpha = 0.0
        beta = 1.0

    if alpha < 0 or beta < 0:
        raise ValueError("Los pesos de competencia y riesgo cambiario no pueden ser negativos.")

    total = alpha + beta
    if total == 0:
        alpha = 0.0
        beta = 1.0
    else:
        alpha = alpha / total
        beta = beta / total

    return {
        "producto_valido": True,
        "precio_propio_usd": precio_propio,
        "precio_promedio_competencia_usd_final": promedio_competencia,
        "peso_competencia_final": alpha,
        "peso_riesgo_cambiario_final": beta,
        "competencia_disponible": True
    }


def compute_suggested_price_row(row, fx_factor, mercado):
    """
    Fórmula:
    PP_t = PP_t-1 * [ alpha * (PC_t-1 / PP_t-1) + beta * fx_factor ]
    """
    resolved = _resolve_weights_and_competition(row, mercado)

    if not resolved["producto_valido"]:
        return {
            "producto_valido": False,
            "precio_sugerido_usd": None,
            "variacion_precio_pct": None,
            "precio_promedio_competencia_usd_final": None,
            "peso_competencia_final": None,
            "peso_riesgo_cambiario_final": None,
            "competencia_disponible": False,
            "motivo": resolved.get("motivo", "")
        }

    pp_prev = resolved["precio_propio_usd"]
    pc_prev = resolved["precio_promedio_competencia_usd_final"]
    alpha = resolved["peso_competencia_final"]
    beta = resolved["peso_riesgo_cambiario_final"]

    competitive_factor = safe_div(pc_prev, pp_prev, default=1.0)
    correction_factor = (alpha * competitive_factor) + (beta * fx_factor)
    suggested_price = pp_prev * correction_factor
    variacion_pct = ((suggested_price - pp_prev) / pp_prev) * 100

    return {
        "producto_valido": True,
        "precio_sugerido_usd": round(float(suggested_price), 4),
        "variacion_precio_pct": round(float(variacion_pct), 2),
        "precio_promedio_competencia_usd_final": round(float(pc_prev), 4),
        "peso_competencia_final": round(float(alpha), 6),
        "peso_riesgo_cambiario_final": round(float(beta), 6),
        "competencia_disponible": resolved["competencia_disponible"],
        "motivo": ""
    }


def apply_pricing_engine(df, mercado, tcm_t, tcbc_t, tcm_t_1, tcbc_t_1):
    """
    Aplica el motor completo a un DataFrame de mercado.
    """
    df = df.copy()

    fx_factor = compute_fx_factor(
        tcm_t=tcm_t,
        tcbc_t=tcbc_t,
        tcm_t_1=tcm_t_1,
        tcbc_t_1=tcbc_t_1
    )

    # Solo productos con precio propio válido
    df["precio_propio_usd"] = pd.to_numeric(df["precio_propio_usd"], errors="coerce")
    df = df[df["precio_propio_usd"].notna() & (df["precio_propio_usd"] > 0)].copy()
    df = df.reset_index(drop=True)

    if df.empty:
        df["fx_factor"] = fx_factor
        df["precio_sugerido_usd"] = None
        df["variacion_precio_pct"] = None
        df["precio_promedio_competencia_usd_final"] = None
        df["peso_competencia_final"] = None
        df["peso_riesgo_cambiario_final"] = None
        df["competencia_disponible"] = False
        df["producto_valido"] = False
        df["motivo"] = "Mercado sin productos con precio propio"
        return df, fx_factor

    resultados = df.apply(
        lambda row: compute_suggested_price_row(row, fx_factor, mercado),
        axis=1
    )

    resultados_df = pd.DataFrame(list(resultados))

    df["fx_factor"] = fx_factor
    df["producto_valido"] = resultados_df["producto_valido"]
    df["precio_sugerido_usd"] = pd.to_numeric(resultados_df["precio_sugerido_usd"], errors="coerce")
    df["variacion_precio_pct"] = pd.to_numeric(resultados_df["variacion_precio_pct"], errors="coerce")
    df["precio_promedio_competencia_usd_final"] = pd.to_numeric(resultados_df["precio_promedio_competencia_usd_final"], errors="coerce")
    df["peso_competencia_final"] = pd.to_numeric(resultados_df["peso_competencia_final"], errors="coerce")
    df["peso_riesgo_cambiario_final"] = pd.to_numeric(resultados_df["peso_riesgo_cambiario_final"], errors="coerce")
    df["competencia_disponible"] = resultados_df["competencia_disponible"]
    df["motivo"] = resultados_df["motivo"]

    return df, fx_factor