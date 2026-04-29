# GeoBusca Territorial Rionegro

Aplicación web para cargar, geocodificar, revisar, clasificar y analizar actividad económica por comunas, barrios, corregimientos y veredas de Rionegro, Antioquia.

## Qué incluye

- carga de datasets CSV y Excel
- geocodificación directa e inversa
- revisión manual de coordenadas
- importación de capas territoriales desde archivo o ArcGIS REST
- clasificación económica
- cruce espacial punto en polígono
- mapa web con filtros
- exportación a CSV, GeoJSON, KML y Excel
- importación de cache histórico de coordenadas
- anexar archivos nuevos a un dataset existente
- módulo de visitas tributarias RVT
- vista imprimible del RVT
- envío automático por correo al guardar la visita
- opción para compartir el RVT por WhatsApp
- modo tablet / campo con formulario por pasos
- firmas táctiles, GPS y evidencias fotográficas
- manifest y service worker básico para instalar como app

## Requisitos

Instala dependencias con:

```bash
pip install -r requirements.txt
```

## Variables de entorno

Copia `.env.example` o define al menos:

```bash
export FLASK_SECRET_KEY='cambia-esta-clave'
export FLASK_DEBUG=1
```

Para envío automático por correo configura además:

```bash
export SMTP_HOST='smtp.gmail.com'
export SMTP_PORT=587
export SMTP_USERNAME='tu_usuario'
export SMTP_PASSWORD='tu_password_o_app_password'
export SMTP_FROM_EMAIL='no-reply@rionegro.gov.co'
export SMTP_FROM_NAME='GeoBusca Territorial'
export SMTP_USE_TLS=1
export APP_BASE_URL='http://127.0.0.1:8000'
```

## Ejecutar

```bash
python run_web.py
```

Luego abre `http://127.0.0.1:8000`.


## Despliegue en VPS

### Opción 1: Gunicorn + systemd + Nginx

```bash
pip install -r requirements.txt
export FLASK_SECRET_KEY='cambia-esta-clave'
export APP_BASE_URL='https://tu-dominio'
gunicorn -c gunicorn.conf.py run_web:app
```

Se incluyen estos archivos de apoyo:

- `gunicorn.conf.py`
- `deploy/geobusca.service`
- `deploy/nginx_geobusca.conf`

### Opción 2: Docker Compose

```bash
docker compose up -d --build
```

Archivos incluidos:

- `Dockerfile`
- `docker-compose.yml`
- `.dockerignore`

### Notas

- configura `FLASK_SECRET_KEY`, `APP_BASE_URL` y variables SMTP
- en producción usa un proxy inverso con HTTPS
- el almacenamiento local usa `geobusca_data/`


## Mejoras V14
- Resolución de conflictos campo a campo desde la UI
- Notificaciones por funcionario
- Tablero gerencial para Secretaría/Subsecretaría
- Firma reforzada con métricas de trazos y hash compuesto


## Ruteo vial

La agenda diaria y la asignación pueden usar ruteo vial real. Configura `ROUTING_PROVIDER=osrm` para usar OSRM o `ROUTING_PROVIDER=graphhopper` para usar GraphHopper.

Variables relevantes: `OSRM_BASE_URL`, `GRAPHHOPPER_BASE_URL`, `GRAPHHOPPER_API_KEY`, `ROUTING_PROFILE`, `ROUTING_TIMEOUT_SECONDS`.
