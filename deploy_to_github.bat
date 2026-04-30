@echo off
echo ==========================================
echo   GeoBusca Territorial - Upload to GitHub
echo ==========================================
echo.
echo 1. Agregando cambios...
git add .
echo.
echo 2. Creando commit con las mejoras de RVT y PDFs...
git commit -m "feat: implementacion automatizacion PDF RVT y estabilizacion frontend/dashboard"
echo.
echo 3. Subiendo a GitHub (Main branch)...
git push origin main
echo.
echo ==========================================
echo   PROCESO COMPLETADO
echo ==========================================
pause
