import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

from backend.app import app


if __name__ == '__main__':
    app.run(
        host=os.getenv('FLASK_HOST', '0.0.0.0'),
        port=int(os.getenv('FLASK_PORT', '8000')),
        debug=os.getenv('FLASK_DEBUG', '0') == '1',
    )
