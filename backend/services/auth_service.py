import random
import string
import time
from typing import Optional
from backend.services.event_service import log_audit
from backend.notifications import send_email

# Cache temporal para OTPs: {user_id: {"code": "123456", "expires_at": 1700000000}}
OTP_CACHE: dict[int, dict] = {}
OTP_EXPIRY_SECONDS = 300  # 5 minutos

def generate_otp(user_id: int, user_email: str) -> str:
    """Genera un código de 6 dígitos, lo guarda en caché y lo envía por correo."""
    code = ''.join(random.choices(string.digits, k=6))
    expires_at = time.time() + OTP_EXPIRY_SECONDS
    
    OTP_CACHE[user_id] = {
        "code": code,
        "expires_at": expires_at
    }
    
    # Enviar el correo
    subject = "Tu código de verificación - GeoBusca Territorial"
    body = f"Tu código de acceso es: {code}. Expira en 5 minutos."
    print(f"DEBUG OTP for user {user_id} ({user_email}): {code}")
    success, error = send_email(user_email, subject, body)
    if not success:
        print(f"ERROR enviando email a {user_email}: {error}")

    
    log_audit(user_id, 'generate_otp', 'user', user_id, details={"expiry": expires_at})
    return code

def verify_otp(user_id: int, input_code: str) -> bool:
    """Valida el código ingresado contra el caché."""
    entry = OTP_CACHE.get(user_id)
    if not entry:
        return False
        
    if time.time() > entry["expires_at"]:
        del OTP_CACHE[user_id]
        return False
        
    if entry["code"] == input_code:
        del OTP_CACHE[user_id]
        log_audit(user_id, 'verify_otp_success', 'user', user_id)
        return True
        
    return False
