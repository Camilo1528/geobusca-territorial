# Cambios V8 - impresión, correo y WhatsApp

## Se agregó

- vista imprimible del Registro de Visita Tributaria
- descarga del RVT como HTML
- envío automático por correo al guardar la visita
- campo de correo personal en el formulario de visitas
- botón para compartir el RVT por WhatsApp
- utilitario `notifications.py` para correo y enlaces de compartición

## Flujo nuevo

1. El funcionario diligencia el RVT.
2. Al guardar, se persiste la visita y se intenta enviar copia al correo indicado.
3. El sistema redirige a la vista imprimible.
4. Desde esa vista se puede:
   - imprimir o guardar como PDF desde el navegador
   - descargar HTML
   - compartir por WhatsApp

## Variables nuevas

- `APP_BASE_URL`
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USERNAME`
- `SMTP_PASSWORD`
- `SMTP_FROM_EMAIL`
- `SMTP_FROM_NAME`
- `SMTP_USE_TLS`

## Notas

- El envío de correo funciona solo si SMTP está configurado.
- La compartición por WhatsApp usa un enlace web `wa.me` con resumen del RVT y la URL imprimible.
