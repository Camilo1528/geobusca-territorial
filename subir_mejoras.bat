@echo off
echo Subiendo mejoras de Visitas a GitHub...
git add backend/routes/visits.py frontend/templates/visits.html frontend/static/js/visits.js
git commit -m "Fix: Visit access, new establishment registration and UI options"
git push
echo Proceso completado.
pause
