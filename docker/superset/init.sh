#!/usr/bin/env bash
set -euo pipefail

superset db upgrade
python - <<'PY'
import os
from uuid import UUID

from superset import db, security_manager
from superset.app import create_app

bi_user = os.environ["BI_USER"]
bi_password = os.environ["BI_PASSWORD"]
app = create_app()
with app.app_context():
    from superset.models.core import Database

    if not security_manager.find_user(username=bi_user):
        user = security_manager.add_user(
            bi_user,
            "Admin",
            "Demo",
            "admin@example.com",
            security_manager.find_role("Admin"),
            bi_password,
        )
        if not user:
            raise RuntimeError("cannot create admin")

    connections = {
        "ClickHouse NYC": (
            UUID("bbf2c493-4232-42bf-b81a-9bef28530a93"),
            f"clickhousedb://{bi_user}:{bi_password}@clickhouse:8123/nyc",
        ),
        "PostGIS staging": (
            UUID("4a76440b-7afb-5e02-98d4-240634e96cd3"),
            f"postgresql://{bi_user}:{bi_password}@postgres:5432/nyc",
        ),
    }
    for name, (uuid, uri) in connections.items():
        database = db.session.query(Database).filter_by(uuid=uuid).one_or_none()
        if database is None:
            database = db.session.query(Database).filter_by(database_name=name).one_or_none()
        if database is None:
            database = Database(database_name=name, uuid=uuid)
            db.session.add(database)
        database.uuid = uuid
        database.set_sqlalchemy_uri(uri)
        database.expose_in_sqllab = True
        database.allow_run_async = False
        database.allow_dml = False
    db.session.commit()
PY
superset init
