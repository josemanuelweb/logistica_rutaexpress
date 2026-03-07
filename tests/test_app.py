import importlib
import pathlib
import sys

import pytest
from fastapi.testclient import TestClient

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


@pytest.fixture()
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("SESSION_SECRET", "test-secret")

    if "main" in sys.modules:
        del sys.modules["main"]

    import main

    importlib.reload(main)

    with TestClient(main.app) as client:
        yield client, main


def test_dashboard_requires_login(app_client):
    client, _ = app_client
    response = client.get("/dashboard", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/login"


def test_login_success_and_dashboard_access(app_client):
    client, main = app_client

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("admin@test.local", main.hash_password("secret123")),
    )
    conn.commit()
    conn.close()

    login_response = client.post(
        "/login",
        data={"email": "admin@test.local", "password": "secret123"},
        follow_redirects=False,
    )

    assert login_response.status_code == 303
    assert login_response.headers["location"] == "/dashboard"

    dashboard_response = client.get("/dashboard")
    assert dashboard_response.status_code == 200
    assert "Panel de envíos" in dashboard_response.text


def test_create_envio_authenticated(app_client):
    client, main = app_client

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("ops@test.local", main.hash_password("clave456")),
    )
    conn.commit()
    conn.close()

    client.post(
        "/login",
        data={"email": "ops@test.local", "password": "clave456"},
        follow_redirects=False,
    )

    response = client.post(
        "/nuevo-envio",
        data={
            "cliente": "Marta Perez",
            "telefono": "+54 11 5555 5555",
            "direccion_retiro": "Av. Corrientes 1234",
            "direccion_entrega": "Cabildo 2000",
            "conductor_id": 1,
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/dashboard"

    check_conn = main.get_db()
    envio = check_conn.execute(
        "SELECT cliente, estado, conductor_id FROM envios ORDER BY id DESC LIMIT 1"
    ).fetchone()
    check_conn.close()

    assert envio["cliente"] == "Marta Perez"
    assert envio["estado"] == "pendiente"
    assert envio["conductor_id"] == 1
