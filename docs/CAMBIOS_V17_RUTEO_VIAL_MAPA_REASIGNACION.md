# Cambios V17: ruteo vial, mapa de agenda y reasignación masiva

## Incluido
- Ruteo vial usando proveedor configurable:
  - OSRM (trip/route)
  - GraphHopper (route)
  - fallback geográfico si no hay proveedor disponible
- Plantilla descargable para agenda masiva (`/admin/assignments/template`)
- Mapa de agenda diaria por funcionario con ruta y paradas
- Reasignación masiva desde panel operativo
- Variables de entorno para proveedor de ruteo

## Archivos tocados
- `config.py`
- `geobusca_saas_complete/app.py`
- `geobusca_saas_complete/templates/visit_assignments.html`
- `geobusca_saas_complete/templates/daily_agenda.html`
- `geobusca_saas_complete/templates/staff_panel.html`
- `.env.example`
- `README.md`

## Notas
- OSRM intenta optimizar orden y trazado vial.
- GraphHopper genera ruta vial sobre el orden sugerido si hay API key configurada.
- Si no hay coordenadas en una visita, queda al final del orden sugerido.
