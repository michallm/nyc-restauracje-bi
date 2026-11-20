import os

from flask import Response

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
BI_USER = os.environ["BI_USER"]
BI_PASSWORD = os.environ["BI_PASSWORD"]
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://{BI_USER}:{BI_PASSWORD}@postgres:5432/superset"
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
ENABLE_PROXY_FIX = True
ROW_LIMIT = 100000
SQL_MAX_ROW = 100000


def configure_app(app):
    # superset requests this path even when pwa files are absent
    @app.get("/static/service-worker.js")
    def service_worker():
        return Response("self.addEventListener('fetch', () => {});", mimetype="application/javascript")


FLASK_APP_MUTATOR = configure_app
DECKGL_BASE_MAP = [
    [
        "tile://https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}",
        "Esri jasna",
    ],
    ["tile://https://tile.openstreetmap.org/{z}/{x}/{y}.png", "OpenStreetMap"],
]
