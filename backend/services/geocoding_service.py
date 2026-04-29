import os
import pandas as pd
from typing import Dict, Optional, List
from backend.database_web import get_conn, now_iso
from models import GeocodeResult
from geocoder import BulkGeocoder
from address_ai import auto_fix_address
from normalizer import AddressNormalizer
from config import DEFAULT_COUNTRY, resolve_provider

def normalize_cache_key(normalized_address: str, city: str, region: str, provider: str) -> tuple[str, str, str, str]:
    return (
        str(normalized_address or '').strip().upper(),
        str(city or '').strip().upper(),
        str(region or '').strip().upper(),
        str(provider or '').strip().lower(),
    )

def load_geocode_cache_map(city: str, region: str, provider: str) -> Dict[str, dict]:
    city_norm, region_norm, provider_norm = str(city).strip().upper(), str(region).strip().upper(), str(provider).strip().lower()
    with get_conn() as conn:
        rows = conn.execute(
            '''
            SELECT normalized_address, latitud, longitud, estado_geo, direccion_geocodificada,
                   consulta_usada, geo_score, geo_confianza
            FROM geocode_cache
            WHERE city=? AND region=? AND provider=?
            ''',
            (city_norm, region_norm, provider_norm),
        ).fetchall()
    out: Dict[str, dict] = {}
    for row in rows:
        out[row['normalized_address']] = dict(row)
    return out

def save_geocode_cache_entry(normalized_address: str, city: str, region: str, provider: str, result: GeocodeResult, geo_score: Optional[int] = None, geo_confianza: str = '') -> None:
    norm_addr, city_norm, region_norm, provider_norm = normalize_cache_key(normalized_address, city, region, provider)
    if not norm_addr:
        return
    with get_conn() as conn:
        conn.execute(
            '''
            INSERT INTO geocode_cache (
                normalized_address, city, region, provider, latitud, longitud, estado_geo,
                direccion_geocodificada, consulta_usada, geo_score, geo_confianza, created_at, last_used_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(normalized_address, city, region, provider) DO UPDATE SET
                latitud=excluded.latitud,
                longitud=excluded.longitud,
                estado_geo=excluded.estado_geo,
                direccion_geocodificada=excluded.direccion_geocodificada,
                consulta_usada=excluded.consulta_usada,
                geo_score=excluded.geo_score,
                geo_confianza=excluded.geo_confianza,
                last_used_at=excluded.last_used_at
            ''',
            (norm_addr, city_norm, region_norm, provider_norm, result.latitud, result.longitud, result.estado_geo, result.direccion_geocodificada, result.consulta_usada, geo_score, geo_confianza, now_iso(), now_iso()),
        )

def touch_geocode_cache_entry(normalized_address: str, city: str, region: str, provider: str) -> None:
    norm_addr, city_norm, region_norm, provider_norm = normalize_cache_key(normalized_address, city, region, provider)
    with get_conn() as conn:
        conn.execute('UPDATE geocode_cache SET last_used_at=? WHERE normalized_address=? AND city=? AND region=? AND provider=?', (now_iso(), norm_addr, city_norm, region_norm, provider_norm))

def geocode_result_from_cache(row: dict, provider: str) -> GeocodeResult:
    return GeocodeResult(
        latitud=row.get('latitud'),
        longitud=row.get('longitud'),
        direccion_geocodificada=row.get('direccion_geocodificada') or '',
        estado_geo=row.get('estado_geo') or 'NO_ENCONTRADO',
        proveedor=provider,
        consulta_usada=row.get('consulta_usada') or '',
    )
