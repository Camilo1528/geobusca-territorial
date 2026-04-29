# Guía de Docker - GeoBusca Territorial

Esta guía explica cómo construir la imagen de Docker, probarla localmente y subirla a Docker Hub.

## 1. Prerrequisitos

*   Tener **Docker Desktop** instalado y en ejecución.
*   Tener una cuenta en [Docker Hub](https://hub.docker.com/).

## 2. Probar Localmente con Docker Compose

Antes de subir la imagen, es recomendable probar que todo funcione correctamente:

```bash
# Construir e iniciar el contenedor
docker-compose up -d --build

# Ver logs para asegurar que inició bien
docker-compose logs -f
```

La aplicación debería estar disponible en: `http://localhost:8000`

## 3. Construir y Subir a Docker Hub

Sigue estos pasos desde una terminal en la raíz del proyecto (`C:\proyecto geo`):

### Paso A: Iniciar sesión en Docker Hub
```bash
docker login
```
*(Ingresa tu usuario y contraseña de Docker Hub)*

### Paso B: Construir la imagen
Reemplaza `TU_USUARIO` con tu nombre de usuario real de Docker Hub.

```bash
# Construir la imagen con un tag (etiqueta)
docker build -t TU_USUARIO/geobusca-territorial:latest .
```

### Paso C: Subir la imagen (Push)
```bash
docker push TU_USUARIO/geobusca-territorial:latest
```

## 4. Notas Importantes sobre los Datos

*   **Persistencia:** La base de datos y los archivos cargados se guardan en `backend/geobusca_data` en tu máquina local. Gracias al mapeo de volúmenes en `docker-compose.yml`, estos datos no se borran al detener el contenedor.
*   **Producción:** Si vas a desplegar esto en un servidor real, asegúrate de que el archivo `.env` tenga las credenciales SMTP y la `SECRET_KEY` adecuadas.
