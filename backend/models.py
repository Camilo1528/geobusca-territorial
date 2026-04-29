from dataclasses import dataclass
from typing import Optional


@dataclass
class GeocodeResult:
    latitud: Optional[float] = None
    longitud: Optional[float] = None
    direccion_geocodificada: str = ""
    score: str = ""
    estado_geo: str = "PENDIENTE"
    error_geo: str = ""
    proveedor: str = ""
    consulta_usada: str = ""
