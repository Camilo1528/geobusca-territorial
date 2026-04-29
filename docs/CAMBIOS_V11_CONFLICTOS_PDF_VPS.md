# Cambios V11

## Mejoras implementadas

- Resolucion de conflictos en guardado de visitas mediante `server_updated_at` y sobrescritura controlada.
- Cola local con estado visual para tablet: `queued`, `syncing`, `synced`, `conflict`, `error`.
- Reintentos manuales y opcion de sobrescribir desde la cola local.
- Descarga de PDF en backend del RVT (`/download.pdf`) usando ReportLab.
- Ajustes para despliegue en VPS:
  - `gunicorn.conf.py`
  - `Dockerfile`
  - `docker-compose.yml`
  - `deploy/geobusca.service`
  - `deploy/nginx_geobusca.conf`
  - `run_web.py` listo para `0.0.0.0:8000`
  - `ProxyFix` en Flask para proxy inverso.

## Notas

- El PDF backend es funcional, pero no replica pixel a pixel el formato fisico; genera un PDF institucional con los campos principales del RVT.
- La resolucion de conflictos aplica al modulo de visitas y sincronizacion desde tablet.
- El despliegue a VPS queda preparado a nivel de estructura y configuracion, pero no se pudo probar en este entorno porque faltaba `flask` instalado en runtime.
