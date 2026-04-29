# GeoBusca v5 - Web integrado y limpio

## Qué quedó integrado desde desktop

- geocodificación directa en el flujo web
- geocodificación inversa desde la pantalla de revisión
- anexar archivos adicionales a un dataset existente
- importación de histórico a la cache SQLite del entorno web
- revisión manual de coordenadas dentro de la app web

## Limpieza aplicada

- se retiraron archivos de escritorio y artefactos generados automáticamente
- se eliminaron logs, HTML exportados, JSON temporales y __pycache__
- se simplificó requirements a dependencias del entorno web
- se dejó un README y un launcher simple para la app web

## Archivos principales

- `geobusca_saas_complete/app.py`: rutas web y lógica de integración
- `geobusca_saas_complete/templates/dashboard.html`: dashboard con capas, ArcGIS y carga histórica
- `geobusca_saas_complete/templates/review.html`: procesamiento, geocodificación inversa y anexos de archivo
- `territorial.py`: cruce espacial y asignación territorial
- `arcgis_rest.py`: importación ArcGIS REST
