import logging
import time
from typing import Dict, List

import requests
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim

from models import GeocodeResult


class BulkGeocoder:
    def __init__(
        self,
        provider: str = "nominatim",
        api_key: str = "",
        user_agent: str = "geocoder_app",
        min_delay_seconds: float = 1.1,
        timeout: int = 15,
        max_retries: int = 3,
    ):
        self.provider = provider.lower().strip()
        self.api_key = api_key.strip()
        self.user_agent = user_agent
        self.min_delay_seconds = min_delay_seconds
        self.timeout = timeout
        self.max_retries = max_retries
        self.cache: Dict[str, GeocodeResult] = {}
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})

        if self.provider == "nominatim":
            self._geolocator = Nominatim(
                user_agent=self.user_agent, timeout=self.timeout)
            self._geocode_fn = RateLimiter(
                self._geolocator.geocode,
                min_delay_seconds=self.min_delay_seconds,
                swallow_exceptions=False,
            )
            self._reverse_fn = RateLimiter(
                self._geolocator.reverse,
                min_delay_seconds=self.min_delay_seconds,
                swallow_exceptions=False,
            )

    def geocode(self, address: str) -> GeocodeResult:
        if not address:
            return GeocodeResult(
                estado_geo="SIN_DIRECCION",
                error_geo="Dirección vacía",
                proveedor=self.provider,
            )
        if address in self.cache:
            return self.cache[address]

        result = GeocodeResult(proveedor=self.provider, consulta_usada=address)
        for attempt in range(1, self.max_retries + 1):
            try:
                if self.provider == "google":
                    result = self._geocode_google(address)
                elif self.provider == "locationiq":
                    result = self._geocode_locationiq(address)
                else:
                    result = self._geocode_nominatim(address)
                result.consulta_usada = address
                self.cache[address] = result
                return result
            except Exception as exc:
                logging.error("Geocoding error on '%s': %s", address, exc)
                result = GeocodeResult(
                    estado_geo="ERROR",
                    error_geo=f"Intento {attempt}: {exc}",
                    proveedor=self.provider,
                    consulta_usada=address,
                )
                if attempt < self.max_retries:
                    time.sleep(min(2 * attempt, 5))
        return result

    def geocode_with_fallbacks(self, candidates: List[str]) -> GeocodeResult:
        last_result = GeocodeResult(
            estado_geo="NO_ENCONTRADO",
            error_geo="Ninguna variante produjo resultado",
            proveedor=self.provider,
        )
        for address in candidates:
            if not address:
                continue
            result = self.geocode(address)
            if result.estado_geo == "OK" and result.latitud is not None:
                return result
            last_result = result
        return last_result

    def reverse_geocode(self, lat: float, lon: float) -> str:
        try:
            if self.provider == "nominatim":
                loc = self._reverse_fn(
                    f"{lat}, {lon}", exactly_one=True, language="es")
                return loc.address if loc else "No encontrado"
            if self.provider == "google":
                data = self._request_json(
                    "https://maps.googleapis.com/maps/api/geocode/json",
                    {"latlng": f"{lat},{lon}", "key": self.api_key, "language": "es"},
                )
                return data["results"][0]["formatted_address"] if data.get(
                    "status") == "OK" else "No encontrado"
            if self.provider == "locationiq":
                data = self._request_json(
                    "https://us1.locationiq.com/v1/reverse.php",
                    {"key": self.api_key, "lat": lat,
                        "lon": lon, "format": "json"},
                )
                return data.get("display_name", "No encontrado")
        except Exception as exc:
            logging.error("Reverse geocode error: %s", exc)
        return "Error de conexión"

    def _request_json(self, url: str, params: Dict[str, object]) -> object:
        response = self.session.get(url, params=params, timeout=self.timeout)
        if response.status_code == 429:
            raise RuntimeError("Rate limit excedido")
        response.raise_for_status()
        return response.json()

    def _geocode_nominatim(self, address: str) -> GeocodeResult:
        location = self._geocode_fn(
            address,
            exactly_one=True,
            addressdetails=True,
            language="es")
        if not location:
            return GeocodeResult(
                estado_geo="NO_ENCONTRADO",
                error_geo="No hay coordenadas",
                proveedor="nominatim",
            )
        return GeocodeResult(
            latitud=float(location.latitude),
            longitud=float(location.longitude),
            direccion_geocodificada=getattr(location, "address", "") or "",
            score="N/A",
            estado_geo="OK",
            proveedor="nominatim",
        )

    def _geocode_google(self, address: str) -> GeocodeResult:
        if not self.api_key:
            return GeocodeResult(estado_geo="ERROR",
                                 error_geo="Falta API Key", proveedor="google")
        data = self._request_json(
            "https://maps.googleapis.com/maps/api/geocode/json",
            {"address": address, "key": self.api_key,
                "language": "es", "region": "co"},
        )
        status = data.get("status", "")
        if status != "OK":
            return GeocodeResult(
                estado_geo="NO_ENCONTRADO" if status == "ZERO_RESULTS" else "ERROR",
                error_geo=f"Google status: {status}",
                proveedor="google",
            )
        item = data["results"][0]
        return GeocodeResult(
            latitud=float(item["geometry"]["location"]["lat"]),
            longitud=float(item["geometry"]["location"]["lng"]),
            direccion_geocodificada=item.get("formatted_address", ""),
            score=item.get("place_id", ""),
            estado_geo="OK",
            proveedor="google",
        )

    def _geocode_locationiq(self, address: str) -> GeocodeResult:
        if not self.api_key:
            return GeocodeResult(
                estado_geo="ERROR", error_geo="Falta API Key", proveedor="locationiq")
        data = self._request_json(
            "https://us1.locationiq.com/v1/search.php",
            {"key": self.api_key,
             "q": address,
             "format": "json",
             "limit": 1,
             "addressdetails": 1},
        )
        if isinstance(data, dict) and data.get("error"):
            return GeocodeResult(estado_geo="ERROR", error_geo=str(
                data.get("error")), proveedor="locationiq")
        if not isinstance(data, list) or not data:
            return GeocodeResult(
                estado_geo="NO_ENCONTRADO", error_geo="Sin resultados", proveedor="locationiq")
        item = data[0]
        return GeocodeResult(
            latitud=float(item["lat"]),
            longitud=float(item["lon"]),
            direccion_geocodificada=item.get("display_name", ""),
            score=str(item.get("importance", "")),
            estado_geo="OK",
            proveedor="locationiq",
        )
