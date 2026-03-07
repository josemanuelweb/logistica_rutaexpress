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


def test_create_conductor_authenticated(app_client):
    client, main = app_client

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("admin2@test.local", main.hash_password("adminpass")),
    )
    conn.commit()
    conn.close()

    client.post(
        "/login",
        data={"email": "admin2@test.local", "password": "adminpass"},
        follow_redirects=False,
    )

    response = client.post(
        "/conductores",
        data={"nombre": "Carlos Ruiz", "telefono": "+54 11 4333 2222"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/conductores"

    check_conn = main.get_db()
    conductor = check_conn.execute(
        "SELECT nombre, telefono FROM conductores WHERE nombre = ?",
        ("Carlos Ruiz",),
    ).fetchone()
    check_conn.close()

    assert conductor["nombre"] == "Carlos Ruiz"
    assert conductor["telefono"] == "+54 11 4333 2222"


def test_edit_conductor_authenticated(app_client):
    client, main = app_client

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("admin3@test.local", main.hash_password("adminpass")),
    )
    conn.execute(
        "INSERT INTO conductores (nombre, telefono) VALUES (?, ?)",
        ("Mario Lopez", "+54 11 4000 1000"),
    )
    conductor_id = conn.execute(
        "SELECT id FROM conductores WHERE nombre = ?",
        ("Mario Lopez",),
    ).fetchone()["id"]
    conn.commit()
    conn.close()

    client.post(
        "/login",
        data={"email": "admin3@test.local", "password": "adminpass"},
        follow_redirects=False,
    )

    response = client.post(
        f"/conductores/{conductor_id}/editar",
        data={"nombre": "Mario L.", "telefono": "+54 11 4999 2000"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/conductores"

    check_conn = main.get_db()
    conductor = check_conn.execute(
        "SELECT nombre, telefono FROM conductores WHERE id = ?",
        (conductor_id,),
    ).fetchone()
    check_conn.close()

    assert conductor["nombre"] == "Mario L."
    assert conductor["telefono"] == "+54 11 4999 2000"


def test_delete_conductor_unassigns_envios(app_client):
    client, main = app_client

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("admin4@test.local", main.hash_password("adminpass")),
    )
    conn.execute(
        "INSERT INTO conductores (nombre, telefono) VALUES (?, ?)",
        ("Laura Diaz", "+54 11 4666 7777"),
    )
    conductor_id = conn.execute(
        "SELECT id FROM conductores WHERE nombre = ?",
        ("Laura Diaz",),
    ).fetchone()["id"]
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, estado, conductor_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("Cliente X", "1111", "A", "B", "pendiente", conductor_id),
    )
    envio_id = conn.execute("SELECT id FROM envios ORDER BY id DESC LIMIT 1").fetchone()["id"]
    conn.commit()
    conn.close()

    client.post(
        "/login",
        data={"email": "admin4@test.local", "password": "adminpass"},
        follow_redirects=False,
    )

    response = client.post(
        f"/conductores/{conductor_id}/eliminar",
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/conductores"

    check_conn = main.get_db()
    conductor = check_conn.execute(
        "SELECT id FROM conductores WHERE id = ?",
        (conductor_id,),
    ).fetchone()
    envio = check_conn.execute(
        "SELECT conductor_id FROM envios WHERE id = ?",
        (envio_id,),
    ).fetchone()
    check_conn.close()

    assert conductor is None
    assert envio["conductor_id"] is None


def test_legacy_conductor_routes_redirect_to_unified_page(app_client):
    client, main = app_client

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("admin5@test.local", main.hash_password("adminpass")),
    )
    conductor_id = conn.execute(
        "SELECT id FROM conductores ORDER BY id ASC LIMIT 1"
    ).fetchone()["id"]
    conn.commit()
    conn.close()

    client.post(
        "/login",
        data={"email": "admin5@test.local", "password": "adminpass"},
        follow_redirects=False,
    )

    acceso = client.get("/acceso-conductor", follow_redirects=False)
    assert acceso.status_code == 303
    assert acceso.headers["location"] == "/conductores"

    panel = client.get(f"/conductor/{conductor_id}", follow_redirects=False)
    assert panel.status_code == 303
    assert panel.headers["location"] == f"/conductores?conductor={conductor_id}"
