# CAMBIOS V12 - roles, sincronización, conflictos, firmas y anexos

## Mejoras implementadas

- Roles más finos:
  - `funcionario`
  - `revisor`
  - `admin`
- El primer usuario registrado queda como `admin`.
- Panel de usuarios para administrar roles.
- Panel de sincronización por funcionario con eventos recientes.
- Bitácora de conflictos con opción de resolución.
- Firmas más robustas:
  - hash SHA-256
  - fecha de firma
  - nombre del firmante
  - metadatos de dispositivo/GPS
- Subida de anexos múltiples por visita.
- Vista imprimible del RVT mostrando anexos y metadatos de firma.
- Registro de eventos de sincronización y conflictos en SQLite.

## Archivos principales modificados

- `database_web.py`
- `institutional.py`
- `geobusca_saas_complete/app.py`
- `geobusca_saas_complete/templates/base.html`
- `geobusca_saas_complete/templates/visits.html`
- `geobusca_saas_complete/templates/visit_print.html`
- `geobusca_saas_complete/templates/admin_users.html`
- `geobusca_saas_complete/templates/sync_panel.html`
- `geobusca_saas_complete/templates/conflict_log.html`

## Notas

- La cola offline sigue funcionando para la visita; los anexos múltiples offline se guardan en cola solamente para imágenes comprimidas capturadas en la tablet.
- Para archivos PDF múltiples sin conexión, el comportamiento depende del navegador y del almacenamiento local disponible.
- El despliegue en VPS sigue siendo compatible con la versión anterior.
