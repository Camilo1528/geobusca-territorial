# CAMBIOS V10 - Modo campo offline y sincronizacion

- Cola offline local para visitas en tablet.
- Sincronizacion manual y automatica al recuperar internet.
- Compresion local de fotos antes del guardado o la cola offline.
- Endpoint JSON de sincronizacion `POST /api/visits/<dataset_id>/sync_save`.
- Soporte en backend para fotos de visita enviadas como data URL.
- Service worker mejorado con cache basico network-first.

## Alcance
Esta version mantiene todo el programa existente y mejora el modulo de visitas de campo para tablets con conectividad intermitente.

## Limitaciones
- El modo offline requiere haber abierto antes la app con sesion iniciada.
- El cache del service worker es basico; no sustituye un motor completo de sincronizacion PWA.
- El PDF sigue dependiendo de la impresion del navegador.
