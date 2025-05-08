# superset_config.py
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://postgres:postgres@postgres:5432/postgres"
SECRET_KEY = "your_secure_key_here"  # Cambia esto, ver paso 3
# Desactiva SQLite explícitamente
SQLALCHEMY_TRACK_MODIFICATIONS = False
# Otras configuraciones recomendadas
WTF_CSRF_ENABLED = True
CONTENT_SECURITY_POLICY_WARNING = False  # Silencia la advertencia de CSP
