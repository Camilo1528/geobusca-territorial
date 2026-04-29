import logging
import math
import re
from typing import Iterable, List, Set, Tuple

import pandas as pd

from config import (
    ALCALDIA_LAT,
    ALCALDIA_LON,
    DEFAULT_CITY,
    MAX_REASONABLE_DISTANCE_METERS,
    QUALITY_CITY_KEYWORDS,
    REPEATED_COORD_THRESHOLD,
)

INVALID_NAMES = {
    "",
    ".",
    "-",
    "0",
    "00",
    "000",
    "SIN NOMBRE",
    "N/A",
    "NA",
    "NONE"}
INVALID_STATUS_EXPORT = {"SIN_DIRECCION", "ERROR", "NO_ENCONTRADO"}


def _norm(value) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().upper()


def haversine_meters(lat1: float, lon1: float,
                     lat2: float, lon2: float) -> float:
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * \
        math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def detect_city_mentions(
        *values: Iterable[str], current_city: str = DEFAULT_CITY) -> list[str]:
    text = _norm(" ".join(str(v or "") for v in values))
    current_city_norm = _norm(current_city)
    mentions: List[str] = []
    for keyword in QUALITY_CITY_KEYWORDS:
        if keyword == current_city_norm:
            continue
        if re.search(rf"\b{re.escape(keyword)}\b", text):
            mentions.append(keyword.title())
    return sorted(set(mentions))


def _duplicate_keys(df: pd.DataFrame) -> set[tuple[float, float]]:
    if not {"latitud", "longitud"}.issubset(df.columns):
        return set()
    coords = df.dropna(subset=["latitud", "longitud"]).copy()
    if coords.empty:
        return set()
    coords["_coord_key"] = list(zip(coords["latitud"].astype(
        float).round(6), coords["longitud"].astype(float).round(6)))
    counts = coords["_coord_key"].value_counts()
    return set(counts[counts >= REPEATED_COORD_THRESHOLD].index.tolist())


def build_quality_record(
    row: pd.Series,
    repeated_points: set[tuple[float, float]],
    current_city: str = DEFAULT_CITY,
    center_lat: float = ALCALDIA_LAT,
    center_lon: float = ALCALDIA_LON,
) -> dict:
    lat = row.get("latitud")
    lon = row.get("longitud")
    estado = _norm(row.get("estado_geo"))
    nombre = _norm(row.get("nom_establec"))
    direccion = row.get("direccion")
    geocodable = row.get("direccion_geocodable")
    comuna = str(row.get("comuna") or "").strip()
    barrio = str(row.get("barrio") or "").strip()
    vereda = str(row.get("vereda") or "").strip()
    zona_rionegro = _norm(row.get("zona_rionegro"))
    fuera_municipio = _norm(row.get("fuera_municipio"))
    territorial_match_score = pd.to_numeric(
        pd.Series([row.get("territorial_match_score")]), errors='coerce').iloc[0]

    anomalies: list[str] = []
    score = 100
    distance = None

    valid_coords = pd.notna(lat) and pd.notna(lon)
    coord_key = None
    if valid_coords:
        try:
            lat = float(lat)
            lon = float(lon)
            coord_key = (round(lat, 6), round(lon, 6))
            distance = round(
                haversine_meters(
                    center_lat,
                    center_lon,
                    lat,
                    lon),
                1)
            if distance > MAX_REASONABLE_DISTANCE_METERS:
                anomalies.append(
                    f"Fuera del rango razonable ({
                        int(distance)} m)")
                score -= 25
        except Exception as exc:
            logging.debug(f"Error validando coordenadas: {exc}")
            valid_coords = False
            anomalies.append("Coordenadas inválidas")
            score -= 40
    else:
        anomalies.append("Sin coordenadas")
        score -= 40

    if nombre in INVALID_NAMES or (nombre and len(nombre) <= 1):
        anomalies.append("Nombre inválido o vacío")
        score -= 15

    external_cities = detect_city_mentions(
        direccion, geocodable, current_city=current_city)
    if external_cities:
        anomalies.append(
            f"Menciona otra ciudad: {', '.join(external_cities[:3])}")
        score -= 30

    if fuera_municipio == 'SI':
        anomalies.append('Punto fuera del límite municipal')
        score -= 45

    if zona_rionegro == 'RURAL' and barrio:
        anomalies.append('Registro rural con barrio asignado')
        score -= 10

    if zona_rionegro == 'URBANO' and vereda:
        anomalies.append('Registro urbano con vereda asignada')
        score -= 10

    if pd.notna(territorial_match_score) and float(
            territorial_match_score) < 60:
        anomalies.append('Asignación territorial débil')
        score -= 20

    if coord_key in repeated_points and estado not in {
            "EDITADO_MANUALMENTE", "CRUCE_HISTORICO"}:
        anomalies.append("Coordenada repetida muchas veces")
        score -= 15

    if estado == "EDITADO_MANUALMENTE":
        score = max(score, 85)
    if estado == "CRUCE_HISTORICO":
        score = max(score, 80)
    if estado in INVALID_STATUS_EXPORT:
        score = min(score, 20)

    if not comuna and not barrio and not vereda and valid_coords and fuera_municipio != 'SI':
        anomalies.append('Sin asignación territorial')
        score -= 15

    score = max(0, min(100, int(round(score))))
    if score >= 85:
        level = "ALTA"
        color = "green"
    elif score >= 60:
        level = "MEDIA"
        color = "orange"
    else:
        level = "BAJA"
        color = "red"

    geo_exportable = bool(valid_coords and score >=
                          45 and estado not in INVALID_STATUS_EXPORT and fuera_municipio != 'SI')

    if fuera_municipio == 'SI':
        reason_short = 'Fuera de Rionegro'
    elif not valid_coords:
        reason_short = 'Sin coordenadas'
    elif external_cities:
        reason_short = 'Posible ciudad distinta'
    elif coord_key in repeated_points:
        reason_short = 'Coordenada repetida'
    elif not comuna and not barrio and not vereda:
        reason_short = 'Sin territorio'
    else:
        reason_short = 'Aprobado'

    return {
        "distancia_alcaldia_m": distance,
        "ciudades_detectadas": ", ".join(external_cities),
        "anomalias_geo": " | ".join(anomalies),
        "geo_anomalia": "SI" if anomalies else "NO",
        "geo_score": int(score),
        "geo_confianza": level,
        "geo_color": color,
        "geo_exportable": geo_exportable,
        "geo_exportable_strict": geo_exportable,
        "geo_reason_short": reason_short,
        "coord_duplicate_drop": False,
    }


def apply_quality_flags(
    df: pd.DataFrame,
    current_city: str = DEFAULT_CITY,
    center_lat: float = ALCALDIA_LAT,
    center_lon: float = ALCALDIA_LON,
) -> pd.DataFrame:
    result = df.copy()
    base_columns = [
        "distancia_alcaldia_m",
        "ciudades_detectadas",
        "anomalias_geo",
        "geo_anomalia",
        "geo_score",
        "geo_confianza",
        "geo_color",
        "geo_exportable",
        "geo_exportable_strict",
        "geo_reason_short",
        "coord_duplicate_drop",
    ]
    if result.empty:
        for col in base_columns:
            if col not in result.columns:
                result[col] = pd.Series(dtype="object")
        return result

    repeated_points = _duplicate_keys(result)
    records = [
        build_quality_record(
            row,
            repeated_points,
            current_city=current_city,
            center_lat=center_lat,
            center_lon=center_lon)
        for _, row in result.iterrows()
    ]
    quality_df = pd.DataFrame(records, index=result.index)
    for column in quality_df.columns:
        result[column] = quality_df[column]
    return result


def deduplicate_suspicious_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if result.empty or not {"latitud", "longitud"}.issubset(result.columns):
        if "coord_duplicate_drop" not in result.columns:
            result["coord_duplicate_drop"] = False
        if "geo_exportable_strict" not in result.columns:
            result["geo_exportable_strict"] = result.get(
                "geo_exportable", False)
        return result

    result["coord_duplicate_drop"] = result.get(
        "coord_duplicate_drop", False).fillna(False).astype(bool)
    result["geo_exportable_strict"] = result.get(
        "geo_exportable_strict", result.get(
            "geo_exportable", False)).fillna(False).astype(bool)

    repeated_points = _duplicate_keys(result)
    if not repeated_points:
        return result

    seen = set()
    for idx, row in result.iterrows():
        if pd.isna(row.get("latitud")) or pd.isna(row.get("longitud")):
            continue
        key = (
            round(
                float(
                    row["latitud"]), 6), round(
                float(
                    row["longitud"]), 6))
        if key not in repeated_points:
            continue
        estado = _norm(row.get("estado_geo"))
        if estado in {"EDITADO_MANUALMENTE", "CRUCE_HISTORICO"}:
            continue
        if key in seen:
            result.at[idx, "coord_duplicate_drop"] = True
            result.at[idx, "geo_exportable_strict"] = False
            reason = str(result.at[idx, "geo_reason_short"] or "").strip()
            result.at[idx,
                      "geo_reason_short"] = f"{reason} / duplicado" if reason else "Duplicado"
        else:
            seen.add(key)

    return result


def filter_exportable(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    result = df.copy()
    export_col = "geo_exportable_strict" if "geo_exportable_strict" in result.columns else "geo_exportable"
    if export_col in result.columns:
        result = result[result[export_col].fillna(False)]
    elif "estado_geo" in result.columns:
        result = result[~result["estado_geo"].astype(
            str).str.upper().isin(INVALID_STATUS_EXPORT)]
    if 'fuera_municipio' in result.columns:
        result = result[result['fuera_municipio'].fillna(
            'NO').astype(str).str.upper() != 'SI']
    return result.dropna(subset=["latitud", "longitud"]).copy()


def quality_summary(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"total": 0, "alta": 0, "media": 0, "baja": 0, "anomalias": 0}
    conf = df.get(
        "geo_confianza",
        pd.Series(
            index=df.index,
            dtype="object")).astype(str).str.upper()
    anom = df.get(
        "geo_anomalia",
        pd.Series(
            index=df.index,
            dtype="object")).astype(str).str.upper()
    return {
        "total": int(len(df)),
        "alta": int((conf == "ALTA").sum()),
        "media": int((conf == "MEDIA").sum()),
        "baja": int((conf == "BAJA").sum()),
        "anomalias": int((anom == "SI").sum()),
    }
