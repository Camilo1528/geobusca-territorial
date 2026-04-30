# INFORME TÉCNICO: ARQUITECTURA GEOBUSCA v2.0
**Documentación de Ingeniería y Sistemas**

## 1. ESPECIFICACIONES DEL STACK
*   **Lenguaje:** Python 3.12.
*   **Framework:** Flask.
*   **Motor de Datos:** SQLite 3 (WAL Mode).
*   **Procesamiento Espacial:** GeoPandas, Shapely, Fiona.

## 2. COMPONENTES CRÍTICOS
*   **Motor Híbrido:** Soporta Google Maps, Nominatim y LocationIQ.
*   **Cruce Territorial:** Algoritmos Point-in-Polygon optimizados con STRtree.
*   **Seguridad:** Hashing PBKDF2, Sesiones JWT y 2FA vía SMTP.

## 3. INFRAESTRUCTURA
*   **Contenedores:** Docker (Debian Bookworm).
*   **Despliegue:** Compatible con Windows Local y Umbrel OS (Linux).
*   **Persistencia:** Gestión de volúmenes para datos y uploads.
