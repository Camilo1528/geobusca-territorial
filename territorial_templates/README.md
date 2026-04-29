# Guía para capas territoriales

Carga preferida:
- `limite_municipal`: polígono del municipio de Rionegro
- `comuna`: polígonos urbanos de comunas
- `barrio`: polígonos de barrios
- `vereda`: polígonos rurales

Formatos recomendados:
- GeoJSON
- Shapefile comprimido en ZIP

Campos sugeridos dentro de la capa:
- `nombre`
- `codigo`
- `comuna` (cuando una capa de barrios tenga referencia a la comuna)

El sistema intentará detectar automáticamente el nombre y código del territorio.
