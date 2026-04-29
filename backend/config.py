import logging
import os
from pathlib import Path
from typing import Tuple

BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = BASE_DIR / 'geobusca_data' / 'app.log'
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

APP_TITLE = 'GeoBusca Territorial Rionegro'
DEFAULT_CITY = 'Rionegro'
DEFAULT_REGION = 'Antioquia'
DEFAULT_COUNTRY = 'Colombia'
DEFAULT_PROVIDER = 'nominatim'
SUPPORTED_PROVIDERS = ('nominatim', 'google', 'locationiq')
PROVIDERS_WITH_API_KEY = {'google', 'locationiq'}

ALCALDIA_LAT = 6.1530
ALCALDIA_LON = -75.3742
MAX_REASONABLE_DISTANCE_METERS = 25000
REPEATED_COORD_THRESHOLD = 4
QUALITY_CITY_KEYWORDS = (
    'RIONEGRO', 'MEDELLIN', 'ENVIGADO', 'BELLO', 'ITAGUI', 'SABANETA', 'LA ESTRELLA',
    'COPACABANA', 'GIRARDOTA', 'CALDAS', 'LA CEJA', 'EL RETIRO', 'GUARNE', 'MARINILLA',
    'EL CARMEN DE VIBORAL', 'SAN VICENTE', 'CONCEPCION', 'SONSON', 'BOGOTA'
)

TERRITORIAL_LAYER_TYPES = (
    'limite_municipal',
    'comuna',
    'barrio',
    'corregimiento',
    'vereda',
    'zona_urbana',
    'zona_rural',
)

TERRITORIAL_NAME_KEYS = {
    'limite_municipal': ['nombre', 'name', 'municipio', 'nom_mpio', 'mun_nombre'],
    'comuna': ['nombre', 'name', 'comuna', 'nom_comuna', 'nombre_comuna', 'unidad'],
    'barrio': ['nombre', 'name', 'barrio', 'nom_barrio', 'nombre_barrio', 'sector'],
    'corregimiento': ['nombre', 'name', 'corregimiento', 'nom_correg', 'nombre_corregimiento', 'centro_poblado'],
    'vereda': ['nombre', 'name', 'vereda', 'nom_vereda', 'nombre_vereda', 'sector'],
    'zona_urbana': ['nombre', 'name', 'nmg', 'uso', 'descripcion', 'clase'],
    'zona_rural': ['nombre', 'name', 'nmg', 'uso', 'descripcion', 'clase'],
}


ROUTING_PROVIDER = os.getenv('ROUTING_PROVIDER', 'osrm').strip().lower()
ROUTING_PROFILE = os.getenv('ROUTING_PROFILE', 'driving').strip().lower()
OSRM_BASE_URL = os.getenv(
    'OSRM_BASE_URL',
    'https://router.project-osrm.org').strip().rstrip('/')
GRAPHHOPPER_BASE_URL = os.getenv(
    'GRAPHHOPPER_BASE_URL',
    'https://graphhopper.com/api/1').strip().rstrip('/')
GRAPHHOPPER_API_KEY = os.getenv('GRAPHHOPPER_API_KEY', '').strip()
ROUTING_TIMEOUT_SECONDS = int(os.getenv('ROUTING_TIMEOUT_SECONDS', '20') or 20)
ALERT_GRACE_MINUTES = 15
ALERT_REPEAT_MINUTES = 30

ROLE_LABELS = {
    "funcionario": "Funcionario",
    "analyst": "Funcionario",
    "revisor": "Revisor",
    "admin": "Administrador"
}
ROLE_RANK = {"funcionario": 10, "analyst": 10, "revisor": 20, "admin": 30}

MODULE_LABELS = {
    'dashboard': 'Tablero',
    'datasets': 'Datasets',
    'process': 'Procesamiento',
    'layers': 'Capas territoriales',
    'review': 'Revisión',
    'map': 'Mapa',
    'visits': 'Visitas',
    'history': 'Histórico',
    'debts': 'Deuda',
    'sync_panel': 'Sincronización',
    'conflicts': 'Conflictos',
    'admin_users': 'Usuarios',
    'staff_panel': 'Panel operativo',
    'notifications': 'Notificaciones',
    'manager_dashboard': 'Tablero gerencial',
    'approval_queue': 'Aprobaciones',
    'visit_assignments': 'Asignación de visitas',
    'daily_agenda': 'Agenda diaria',
    'supervision_live': 'Supervisión en tiempo real',
    'exports': 'Exportes',
    'api': 'API institucional',
}

ROLE_DEFAULT_MODULES = {
    'funcionario': {'dashboard', 'datasets', 'process', 'map', 'visits', 'history', 'exports', 'staff_panel', 'notifications', 'daily_agenda'},
    'analyst': {'dashboard', 'datasets', 'process', 'map', 'visits', 'history', 'exports', 'staff_panel', 'notifications', 'daily_agenda'},
    'revisor': set(MODULE_LABELS.keys()) - {'admin_users'},
    'admin': set(MODULE_LABELS.keys()),
}

APPROVAL_REASON_OPTIONS = {
    'approve': [
        ('datos_validados', 'Datos validados'),
        ('visita_cerrada', 'Visita cerrada correctamente'),
        ('deuda_validada', 'Deuda validada'),
        ('soportes_completos', 'Soportes completos'),
    ],
    'return': [
        ('informacion_incompleta', 'Información incompleta'),
        ('falta_soporte', 'Falta soporte o evidencia'),
        ('firma_invalida', 'Firma inválida o incompleta'),
        ('gps_inconsistente', 'GPS inconsistente'),
        ('anexos_insuficientes', 'Anexos insuficientes'),
    ],
    'reject': [
        ('visita_no_valida', 'Visita no válida'),
        ('establecimiento_no_identificado', 'Establecimiento no identificado'),
        ('duplicado', 'Registro duplicado'),
        ('territorio_incorrecto', 'Territorio incorrecto'),
        ('inconsistencia_grave', 'Inconsistencia grave'),
    ],
}

ALL_APPROVAL_REASON_CODES = {
    code: label for options in APPROVAL_REASON_OPTIONS.values() for code,
    label in options}

COMPLETION_REASON_OPTIONS = {
    'completed': [('gestion_exitosa', 'Gestión exitosa')],
    'no_effective': [
        ('cerrado', 'Establecimiento cerrado'),
        ('no_atiende', 'No atienden la visita'),
        ('predio_vacio', 'Predio vacío'),
        ('no_ubicado', 'No fue posible ubicarlo'),
        ('negativa_atencion', 'Negativa de atención'),
    ],
    'pending': [('', 'Sin causal')],
}

EVIDENCE_REQUIRED_CAUSES = {
    'cerrado',
    'no_atiende',
    'predio_vacio',
    'no_ubicado',
    'negativa_atencion'}


TERRITORIAL_CODE_KEYS = [
    'codigo', 'code', 'id', 'objectid', 'fid', 'cod', 'cod_dane',
    'codigo_barrio', 'codigo_vereda', 'codigo_comuna', 'codigo_corregimiento',
]

# Capas territoriales precargadas de Rionegro (recomendado para evitar
# dependencia externa)
RIONEGRO_TERRITORIAL_LAYERS = {
    "zona_rural": "geobusca_data/territorial_layers/rionegro/zona_rural.geojson",
    "zona_urbana": "geobusca_data/territorial_layers/rionegro/zona_urbana.geojson"
}


def normalize_provider(provider: str) -> str:
    provider_value = str(provider or DEFAULT_PROVIDER).strip().lower()
    return provider_value if provider_value in SUPPORTED_PROVIDERS else DEFAULT_PROVIDER


def provider_requires_api_key(provider: str) -> bool:
    return normalize_provider(provider) in PROVIDERS_WITH_API_KEY


def resolve_provider(provider: str, api_key: str) -> tuple[str, str]:
    normalized = normalize_provider(provider)
    if provider_requires_api_key(
            normalized) and not str(api_key or '').strip():
        return DEFAULT_PROVIDER, f"El proveedor '{normalized}' requiere API Key. Se usará '{DEFAULT_PROVIDER}'."
    return normalized, ''
