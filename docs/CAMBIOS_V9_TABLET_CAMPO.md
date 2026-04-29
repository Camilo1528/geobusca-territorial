# CAMBIOS V9 - MODO TABLET / CAMPO

## Objetivo
Esta versión conserva todo el flujo web existente y mejora el módulo de visitas para uso real en tablet por parte del funcionario en campo.

## Mejoras implementadas
- vista de visitas rediseñada para tablet, sin perder el resto del sistema
- formulario RVT por pasos
- botones grandes y navegación touch-friendly
- borrador local automático por fila con localStorage
- captura de GPS desde el navegador
- captura de firma táctil en canvas para receptor y funcionario
- carga de fotos del establecimiento y del documento/evidencia
- persistencia de fotos y firmas en el registro de visita
- vista imprimible del RVT con firmas, GPS y evidencias
- manifest web y service worker básico para instalación tipo app

## Archivos principales modificados
- `geobusca_saas_complete/app.py`
- `geobusca_saas_complete/templates/base.html`
- `geobusca_saas_complete/templates/visits.html`
- `geobusca_saas_complete/templates/visit_print.html`
- `institutional.py`
- `README.md`
- `.env.example`

## Observaciones
- el service worker es básico y no reemplaza todavía un modo offline completo
- las firmas se capturan como imagen
- las fotos de visita se almacenan en `geobusca_data/visit_media`
- la impresión / PDF sigue funcionando junto con correo y WhatsApp
