FROM python:3.12-slim-bookworm

# Metadatos
LABEL maintainer="Camilo <camilo152893@gmail.com>"
LABEL description="GeoBusca Territorial - Sistema de geocodificación y análisis territorial"
LABEL version="v2.0"

WORKDIR /app

# Variables de entorno
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    gdal-bin \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar dependencias Python
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Crear usuario no-root para seguridad
RUN useradd -m -u 1001 -s /bin/bash geobusca \
    && chown -R geobusca:geobusca /app

# Copiar código de la aplicación
COPY --chown=geobusca:geobusca . .

# Crear directorios de datos con permisos correctos en la ubicación esperada por el código
RUN mkdir -p /app/backend/geobusca_data/uploads \
    /app/backend/geobusca_data/exports \
    /app/backend/geobusca_data/territorial_layers \
    /app/backend/geobusca_data/visit_media \
    && chown -R geobusca:geobusca /app/backend/geobusca_data

# Puerto de la aplicación
EXPOSE 8000

# Cambiar a usuario no-root
USER geobusca

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Comando de inicio usando gunicorn con la configuración externa
CMD ["gunicorn", "-c", "gunicorn.conf.py", "run_web:app"]
