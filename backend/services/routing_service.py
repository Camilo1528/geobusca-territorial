import os
import requests
import math
from typing import List, Dict, Optional, Tuple
from backend.config import (
    ALCALDIA_LAT, ALCALDIA_LON, OSRM_BASE_URL, GRAPHHOPPER_BASE_URL, 
    GRAPHHOPPER_API_KEY, ROUTING_PROFILE, ROUTING_TIMEOUT_SECONDS
)

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_lat / 2) ** 2 + math.cos(math.radians(lat1)) * \
        math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
    return r * 2 * math.asin(min(1.0, math.sqrt(a)))

def extract_row_coordinates(row: dict) -> tuple[Optional[float], Optional[float]]:
    for lat_key, lon_key in [('visit_latitude', 'visit_longitude'),
                             ('latitud', 'longitud'), ('latitude', 'longitude')]:
        try:
            lat = float(str(row.get(lat_key, '')).strip())
            lon = float(str(row.get(lon_key, '')).strip())
            return lat, lon
        except Exception:
            continue
    return None, None

def nearest_neighbor_order(rows: list[dict], start_lat: float, start_lon: float) -> tuple[list[dict], float]:
    available = [dict(item) for item in rows]
    ordered = []
    total_km = 0.0
    current_lat, current_lon = start_lat, start_lon
    while available:
        next_item = min(
            available,
            key=lambda item: haversine_km(
                current_lat, current_lon,
                item['_route_lat'], item['_route_lon']))
        step = haversine_km(
            current_lat, current_lon,
            next_item['_route_lat'], next_item['_route_lon'])
        total_km += step
        next_item['_route_step_km'] = round(step, 3)
        ordered.append(next_item)
        current_lat, current_lon = next_item['_route_lat'], next_item['_route_lon']
        available.remove(next_item)
    return ordered, round(total_km, 2)

def fetch_osrm_trip_plan(points: list[dict]) -> Optional[dict]:
    if len(points) < 2: return None
    coords = ';'.join(f"{item['_route_lon']},{item['_route_lat']}" for item in points)
    profile = str(os.getenv('ROUTING_PROFILE', ROUTING_PROFILE or 'driving')).strip().lower() or 'driving'
    url = f"{OSRM_BASE_URL}/trip/v1/{profile}/{coords}"
    params = {'source': 'first', 'roundtrip': 'false', 'steps': 'false', 'overview': 'full', 'geometries': 'geojson'}
    try:
        response = requests.get(url, params=params, timeout=ROUTING_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
        if payload.get('code') != 'Ok' or not payload.get('trips'): return None
        trip = payload['trips'][0]
        waypoints = payload.get('waypoints') or []
        ordered_indexes = [None] * len(points)
        for input_index, waypoint in enumerate(waypoints):
            waypoint_index = waypoint.get('waypoint_index')
            if isinstance(waypoint_index, int) and 0 <= waypoint_index < len(points):
                ordered_indexes[waypoint_index] = input_index
        ordered = [dict(points[input_idx]) for input_idx in ordered_indexes if input_idx is not None]
        return {
            'ordered': ordered,
            'total_km': round(float(trip.get('distance') or 0) / 1000.0, 2),
            'total_minutes': int(round(float(trip.get('duration') or 0) / 60.0)) if trip.get('duration') is not None else None,
            'geometry': trip.get('geometry'),
            'provider': 'osrm',
            'method': 'osrm_trip'
        }
    except Exception: return None

def build_route_plan(rows: list[dict], start_lat: float = ALCALDIA_LAT, start_lon: float = ALCALDIA_LON) -> dict:
    available = []
    missing = []
    for idx, item in enumerate(rows):
        row = dict(item)
        lat, lon = extract_row_coordinates(row)
        row['_route_lat'], row['_route_lon'], row['_route_original_index'] = lat, lon, idx
        if lat is None or lon is None: missing.append(row)
        else: available.append(row)
    
    if not available:
        return {'ordered': rows, 'total_km': 0.0, 'total_minutes': None, 'geometry': None, 'provider': 'none', 'method': 'missing_coordinates'}

    plan = fetch_osrm_trip_plan(available)
    if not plan:
        ordered, fallback_total = nearest_neighbor_order(available, start_lat, start_lon)
        plan = {
            'ordered': ordered, 'total_km': fallback_total, 
            'total_minutes': int(round((fallback_total / 25.0) * 60)) if fallback_total > 0 else None,
            'geometry': None, 'provider': 'geographic', 'method': 'nearest_neighbor'
        }
    
    final_order = [dict(item) for item in plan['ordered']] + [dict(item) for item in missing]
    for item in final_order:
        for key in ['_route_lat', '_route_lon', '_route_original_index', '_route_step_km']:
            item.pop(key, None)
    plan['ordered'] = final_order
    plan['missing_count'] = len(missing)
    return plan
