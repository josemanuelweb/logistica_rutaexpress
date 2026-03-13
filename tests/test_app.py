import importlib
import pathlib
import re
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
            "direccion_entrega": "Cabildo 2000",
            "conductor_id": 1,
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == f"/dashboard?fecha={main.today_iso()}"

    check_conn = main.get_db()
    envio = check_conn.execute(
        "SELECT cliente, estado, conductor_id, fecha, direccion_retiro FROM envios ORDER BY id DESC LIMIT 1"
    ).fetchone()
    check_conn.close()

    assert envio["cliente"] == "Marta Perez"
    assert envio["estado"] == "pendiente"
    assert envio["conductor_id"] == 1
    assert envio["fecha"] == main.today_iso()
    assert envio["direccion_retiro"] == ""


def test_create_envio_redirects_to_matching_dashboard_filters(app_client):
    client, main = app_client

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("ops2@test.local", main.hash_password("clave456")),
    )
    conn.commit()
    conn.close()

    client.post(
        "/login",
        data={"email": "ops2@test.local", "password": "clave456"},
        follow_redirects=False,
    )

    response = client.post(
        "/nuevo-envio",
        data={
            "cliente": "Pedido Futuro",
            "telefono": "+54 11 5555 1111",
            "direccion_entrega": "Destino 456",
            "fecha": "2026-03-20",
            "conductor_id": 2,
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/dashboard?fecha=2026-03-20"


def test_dashboard_without_date_filter_shows_historical_records(app_client):
    client, main = app_client

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("admin-history@test.local", main.hash_password("adminpass")),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente Marzo", "1111", "", "Entrega Marzo", "2026-03-10", "pendiente", 1),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente Abril", "2222", "", "Entrega Abril", "2026-04-02", "pendiente", 1),
    )
    conn.commit()
    conn.close()

    client.post(
        "/login",
        data={"email": "admin-history@test.local", "password": "adminpass"},
        follow_redirects=False,
    )

    response = client.get("/dashboard", follow_redirects=False)

    assert response.status_code == 200
    assert "Cliente Marzo" in response.text
    assert "Cliente Abril" in response.text


def test_dashboard_range_filters_and_metrics(app_client):
    client, main = app_client

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("metrics@test.local", main.hash_password("adminpass")),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id, en_ruta_at, entregado_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente M1", "1111", "", "Entrega 1", "2026-03-10", "entregado", 1, "2026-03-10 09:00:00", "2026-03-10 09:30:00"),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente M2", "2222", "", "Entrega 2", "2026-03-12", "pendiente", 2),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente Fuera", "3333", "", "Entrega 3", "2026-04-02", "pendiente", 1),
    )
    conn.commit()
    conn.close()

    client.post(
        "/login",
        data={"email": "metrics@test.local", "password": "adminpass"},
        follow_redirects=False,
    )

    response = client.get("/dashboard?desde=2026-03-01&hasta=2026-03-31", follow_redirects=False)

    assert response.status_code == 200
    assert "Cliente M1" in response.text
    assert "Cliente M2" in response.text
    assert "Cliente Fuera" not in response.text
    assert "Total envíos" in response.text
    assert ">2<" in response.text
    assert "30.0 min" in response.text


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
    assert acceso.headers["location"] == "/dashboard"

    panel = client.get(f"/conductor/{conductor_id}", follow_redirects=False)
    assert panel.status_code == 303
    assert panel.headers["location"] == f"/dashboard?conductor={conductor_id}"


def test_export_hoja_ruta_csv_respects_filters(app_client):
    client, main = app_client

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("admin6@test.local", main.hash_password("adminpass")),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente Ruta", "2222", "Retiro A", "Entrega B", "2026-03-01", "pendiente", 1),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente Otro", "3333", "Retiro X", "Entrega Y", "2026-03-02", "pendiente", 2),
    )
    conn.commit()
    conn.close()

    client.post(
        "/login",
        data={"email": "admin6@test.local", "password": "adminpass"},
        follow_redirects=False,
    )

    response = client.get(
        "/dashboard/hoja-ruta.csv?fecha=2026-03-01&conductor=1",
        follow_redirects=False,
    )

    assert response.status_code == 200
    assert "attachment; filename=" in response.headers["content-disposition"]
    csv_text = response.text
    assert "Cliente Ruta" in csv_text
    assert "Cliente Otro" not in csv_text


def test_generate_route_for_conductor_orders_stops_and_saves_coords(app_client, monkeypatch):
    client, main = app_client

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("admin7@test.local", main.hash_password("adminpass")),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente 1", "111", "R1", "Dir A", "2026-03-03", "pendiente", 1),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente 2", "222", "R2", "Dir B", "2026-03-03", "pendiente", 1),
    )
    conn.commit()
    conn.close()

    coords = {
        "Dir A": (-34.6000, -58.3800),
        "Dir B": (-34.7000, -58.5000),
    }

    monkeypatch.setattr(main, "geocode_address", lambda address: coords.get(address))
    monkeypatch.setattr(
        main,
        "build_batched_road_route",
        lambda route_coords: {
            "path": [[lat, lng] for lat, lng in route_coords],
            "distance_km": 22.5,
            "duration_min": 48.0,
        },
    )

    client.post(
        "/login",
        data={"email": "admin7@test.local", "password": "adminpass"},
        follow_redirects=False,
    )

    response = client.get("/conductores/1/ruta?fecha=2026-03-03", follow_redirects=False)
    assert response.status_code == 200
    assert "Ruta optimizada" in response.text
    assert "Cliente 1" in response.text
    assert "Cliente 2" in response.text
    assert "Tiempo estimado" in response.text

    check_conn = main.get_db()
    saved = check_conn.execute(
        "SELECT COUNT(*) AS qty FROM envios WHERE fecha = ? AND conductor_id = ? AND entrega_lat IS NOT NULL AND entrega_lng IS NOT NULL",
        ("2026-03-03", 1),
    ).fetchone()
    check_conn.close()
    assert saved["qty"] >= 2


def test_build_geocode_queries_add_local_context_and_variants(app_client):
    _, main = app_client

    cabildo_queries = main.build_geocode_queries("Cabildo 2000")
    assert "Cabildo 2000" in cabildo_queries
    assert "Cabildo 2000, Buenos Aires, Argentina" in cabildo_queries

    barracas_queries = main.build_geocode_queries("barracas, Bolivar 1783")
    assert "barracas, Bolivar 1783" in barracas_queries
    assert "Bolivar 1783, barracas" in barracas_queries
    assert "Bolivar 1783, barracas, Buenos Aires, Argentina" in barracas_queries

    las_heras_queries = main.build_geocode_queries("Lasheras 3000, Caba")
    assert any("Las Heras 3000" in query for query in las_heras_queries)
    assert any("Ciudad Autonoma de Buenos Aires" in query for query in las_heras_queries)


def test_generate_route_uses_all_delivery_addresses_in_optimized_order(app_client, monkeypatch):
    client, main = app_client

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("admin8@test.local", main.hash_password("adminpass")),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id, entrega_lat, entrega_lng)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente A", "111", "R1", "Entrega A", "2026-03-04", "pendiente", 1, -34.61, -58.38),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id, entrega_lat, entrega_lng)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente B", "222", "R2", "Entrega B", "2026-03-04", "pendiente", 1, -34.62, -58.39),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id, entrega_lat, entrega_lng)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente C", "333", "R3", "Entrega C", "2026-03-04", "pendiente", 1, -34.63, -58.40),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(
        main,
        "request_osrm_trip",
        lambda route_coords: {
            "order": [0, 2, 1, 3],
            "path": [[lat, lng] for lat, lng in route_coords],
            "distance_km": 12.5,
            "duration_min": 31.0,
        },
    )

    client.post(
        "/login",
        data={"email": "admin8@test.local", "password": "adminpass"},
        follow_redirects=False,
    )

    response = client.get("/conductores/1/ruta?fecha=2026-03-04", follow_redirects=False)

    assert response.status_code == 200
    rows = re.findall(r"<tr>\s*<td>\d+</td>\s*<td>#\d+</td>\s*<td>[^<]+</td>\s*<td>([^<]+)</td>", response.text)
    assert rows[:3] == ["Entrega B", "Entrega C", "Entrega A"]


def test_generate_route_uses_current_origin_coordinates_when_provided(app_client, monkeypatch):
    client, main = app_client

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("admin9@test.local", main.hash_password("adminpass")),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id, entrega_lat, entrega_lng)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente Origen", "111", "R1", "Entrega Unica", "2026-03-05", "pendiente", 1, -34.62, -58.41),
    )
    conn.commit()
    conn.close()

    captured = {}

    def fake_trip(route_coords):
        captured["route_coords"] = route_coords
        return {
            "order": [0, 1],
            "path": [[lat, lng] for lat, lng in route_coords],
            "distance_km": 2.0,
            "duration_min": 5.0,
        }

    monkeypatch.setattr(main, "request_osrm_trip", fake_trip)

    client.post(
        "/login",
        data={"email": "admin9@test.local", "password": "adminpass"},
        follow_redirects=False,
    )

    response = client.get(
        "/conductores/1/ruta?fecha=2026-03-05&origen_lat=-34.55&origen_lng=-58.45",
        follow_redirects=False,
    )

    assert response.status_code == 200
    assert captured["route_coords"][0] == (-34.55, -58.45)
    assert "Ubicacion actual" in response.text


def test_start_route_marks_first_stop_as_en_ruta(app_client, monkeypatch):
    client, main = app_client
    monkeypatch.setattr(main, "current_timestamp", lambda: "2026-03-06 08:15:00")

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("driverflow1@test.local", main.hash_password("adminpass")),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id, entrega_lat, entrega_lng)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente 1", "111", "", "Entrega 1", "2026-03-06", "pendiente", 1, -34.61, -58.38),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id, entrega_lat, entrega_lng)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente 2", "222", "", "Entrega 2", "2026-03-06", "pendiente", 1, -34.62, -58.39),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(
        main,
        "request_osrm_trip",
        lambda route_coords: {
            "order": [0, 1, 2],
            "path": [[lat, lng] for lat, lng in route_coords],
            "distance_km": 5.0,
            "duration_min": 12.0,
        },
    )

    client.post(
        "/login",
        data={"email": "driverflow1@test.local", "password": "adminpass"},
        follow_redirects=False,
    )

    response = client.post(
        "/conductores/1/ruta/iniciar",
        data={"fecha": "2026-03-06"},
        follow_redirects=False,
    )

    assert response.status_code == 303

    check_conn = main.get_db()
    first = check_conn.execute(
        "SELECT estado, en_ruta_at FROM envios WHERE cliente = ?",
        ("Cliente 2",),
    ).fetchone()
    second = check_conn.execute(
        "SELECT estado, en_ruta_at FROM envios WHERE cliente = ?",
        ("Cliente 1",),
    ).fetchone()
    check_conn.close()

    assert first["estado"] == "en_ruta"
    assert first["en_ruta_at"] == "2026-03-06 08:15:00"
    assert second["estado"] == "pendiente"
    assert second["en_ruta_at"] is None


def test_complete_stop_marks_delivered_and_advances_next_stop(app_client, monkeypatch):
    client, main = app_client
    monkeypatch.setattr(main, "current_timestamp", lambda: "2026-03-07 09:45:00")

    conn = main.get_db()
    conn.execute(
        "INSERT INTO users (email, password) VALUES (?, ?)",
        ("driverflow2@test.local", main.hash_password("adminpass")),
    )
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id, entrega_lat, entrega_lng)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente A", "111", "", "Entrega A", "2026-03-07", "en_ruta", 1, -34.61, -58.38),
    )
    first_id = conn.execute("SELECT id FROM envios WHERE cliente = ?", ("Cliente A",)).fetchone()["id"]
    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id, entrega_lat, entrega_lng)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("Cliente B", "222", "", "Entrega B", "2026-03-07", "pendiente", 1, -34.62, -58.39),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(
        main,
        "request_osrm_trip",
        lambda route_coords: {
            "order": [0, 1, 2],
            "path": [[lat, lng] for lat, lng in route_coords],
            "distance_km": 5.0,
            "duration_min": 12.0,
        },
    )

    client.post(
        "/login",
        data={"email": "driverflow2@test.local", "password": "adminpass"},
        follow_redirects=False,
    )

    response = client.post(
        f"/conductores/1/ruta/{first_id}/entregado",
        data={"fecha": "2026-03-07"},
        follow_redirects=False,
    )

    assert response.status_code == 303

    check_conn = main.get_db()
    first = check_conn.execute(
        "SELECT estado, entregado_at FROM envios WHERE id = ?",
        (first_id,),
    ).fetchone()
    second = check_conn.execute(
        "SELECT estado, en_ruta_at FROM envios WHERE cliente = ?",
        ("Cliente B",),
    ).fetchone()
    check_conn.close()

    assert first["estado"] == "entregado"
    assert first["entregado_at"] == "2026-03-07 09:45:00"
    assert second["estado"] == "en_ruta"
    assert second["en_ruta_at"] == "2026-03-07 09:45:00"
