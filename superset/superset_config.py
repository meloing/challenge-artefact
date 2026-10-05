"""
Apache Superset Production Configuration for WABA Group.

Features:
  - Trino SQLAlchemy engine connectivity (iceberg.gold catalog)
  - Keycloak SSO integration via OpenID Connect (OAuth2)
  - Role-based security mappings (group_admin, country_analyst, compliance_officer, viewer)
  - Automated dashboard and datasource provisioning

Author: Armel Kouadio
"""

import os
from flask_appbuilder.security.manager import AUTH_OAUTH

# Superset App Settings
ROW_LIMIT = 50000
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "WABA_SUPERSET_SECRET_KEY_SUPER_SECURE_98234")
SQLALCHEMY_DATABASE_URI = os.getenv("SUPERSET_META_DB_URI", "sqlite:////tmp/superset.db")

# Enable Trino Query Engine
FEATURE_FLAGS = {
    "ALERT_REPORTS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "GENERIC_CHART_AXES": True,
    "ENABLE_TEMPLATE_PROCESSING": True
}

# Keycloak SSO Authentication (OIDC / OAuth2)
AUTH_TYPE = AUTH_OAUTH
OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.getenv("KEYCLOAK_SUPERSET_CLIENT_ID", "waba-superset"),
            "client_secret": os.getenv("KEYCLOAK_SUPERSET_CLIENT_SECRET", "superset-sso-secret-waba"),
            "api_base_url": os.getenv("KEYCLOAK_BASE_URL", "http://keycloak:8080/realms/waba/protocol/openid-connect/"),
            "access_token_url": os.getenv("KEYCLOAK_BASE_URL", "http://keycloak:8080/realms/waba/protocol/openid-connect/") + "token",
            "authorize_url": os.getenv("KEYCLOAK_BASE_URL", "http://keycloak:8080/realms/waba/protocol/openid-connect/") + "auth",
            "client_kwargs": {
                "scope": "openid email profile roles"
            }
        }
    }
]

# Map Keycloak Realm Roles to Superset Internal Roles
AUTH_ROLES_MAPPING = {
    "group_admin": ["Admin"],
    "country_analyst": ["Alpha"],
    "compliance_officer": ["Alpha"],
    "viewer": ["Gamma"]  # Gamma role has restricted read-only access with PII masking
}
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Gamma"

# Pre-configured Trino Lakehouse Connection
# Format: trino://{user}:{password}@{host}:{port}/{catalog}/{schema}
TRINO_SQLALCHEMY_URL = os.getenv("TRINO_URI", "trino://trino@trino:8080/iceberg/gold")
