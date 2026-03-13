import hashlib
import hmac
import io
import json
import math
import os
import re
import sqlite3
from csv import writer
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request as UrlRequest, urlopen

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

ESTADOS_VALIDOS = {"pendiente", "en_ruta", "entregado"}
DATABASE_PATH = os.getenv("DATABASE_PATH", "database.db")
SESSION_SECRET = os.getenv("SESSION_SECRET", "change-this-secret-before-production")

app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    same_site="lax",
    https_only=False,
)


def get_db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def hash_password(password: str, iterations: int = 120_000) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${digest.hex()}"


def verify_password(password: str, stored_value: str) -> bool:
    if not stored_value:
        return False

    if stored_value.startswith("pbkdf2_sha256$"):
        try:
            _, iteration_text, salt_hex, digest_hex = stored_value.split("$", 3)
            iterations = int(iteration_text)
            expected = bytes.fromhex(digest_hex)
            salt = bytes.fromhex(salt_hex)
        except (TypeError, ValueError):
            return False

        candidate = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), salt, iterations
        )
        return hmac.compare_digest(candidate, expected)

    # Compatibilidad con passwords legadas en texto plano.
    return hmac.compare_digest(password, stored_value)


def login_required(request: Request):
    if not request.session.get("user_id"):
        return RedirectResponse("/login", status_code=303)
    return None


def today_iso() -> str:
    return date.today().isoformat()


def current_timestamp() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")


def normalize_date_or_today(raw_date: str) -> str:
    candidate = raw_date.strip()
    if not candidate:
        return today_iso()
    try:
        return datetime.strptime(candidate, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return today_iso()


def normalize_optional_date(raw_date: str) -> str:
    candidate = raw_date.strip()
    if not candidate:
        return ""
    try:
        return datetime.strptime(candidate, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return ""


def calculate_delivery_minutes(started_at: Optional[str], delivered_at: Optional[str]) -> Optional[float]:
    if not started_at or not delivered_at:
        return None
    try:
        started = datetime.fromisoformat(started_at)
        delivered = datetime.fromisoformat(delivered_at)
    except ValueError:
        return None
    delta_min = (delivered - started).total_seconds() / 60.0
    if delta_min < 0:
        return None
    return round(delta_min, 1)


def parse_coordinate(raw_value: str, minimum: float, maximum: float) -> Optional[float]:
    candidate = raw_value.strip()
    if not candidate:
        return None
    try:
        value = float(candidate)
    except ValueError:
        return None
    if value < minimum or value > maximum:
        return None
    return value


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(d_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius * c


def optimize_order(
    points: List[Dict[str, object]], origin_lat: float, origin_lng: float
) -> List[Dict[str, object]]:
    remaining = points[:]
    ordered = []
    current_lat = origin_lat
    current_lng = origin_lng

    while remaining:
        next_point = min(
            remaining,
            key=lambda p: haversine_km(current_lat, current_lng, p["lat"], p["lng"]),
        )
        ordered.append(next_point)
        remaining.remove(next_point)
        current_lat = next_point["lat"]
        current_lng = next_point["lng"]

    return ordered


def normalize_address_text(address: str) -> str:
    query = " ".join(address.strip().split())
    if not query:
        return ""

    replacements = {
        r"\bcaba\b": "Ciudad Autonoma de Buenos Aires",
        r"\bcap\.?\s*federal\b": "Ciudad Autonoma de Buenos Aires",
        r"\bbs\.?\s*as\.?\b": "Buenos Aires",
        r"\blasheras\b": "Las Heras",
    }
    for pattern, replacement in replacements.items():
        query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)
    return query


def with_buenos_aires_context(query: str) -> str:
    lowered = query.lower()
    location_hints = (
        "buenos aires",
        "ciudad autonoma de buenos aires",
        "argentina",
        "capital federal",
    )
    if any(hint in lowered for hint in location_hints):
        return query
    return f"{query}, Buenos Aires, Argentina"


def build_geocode_queries(address: str) -> List[str]:
    base = normalize_address_text(address)
    if not base:
        return []

    candidates: List[str] = []
    seen = set()

    def add(candidate: str) -> None:
        value = " ".join(candidate.strip().split(" ,"))
        if not value:
            return
        key = value.lower()
        if key in seen:
            return
        seen.add(key)
        candidates.append(value)

    add(base)
    add(with_buenos_aires_context(base))

    parts = [part.strip() for part in base.split(",") if part.strip()]
    if len(parts) >= 2:
        first, second = parts[0], parts[1]
        if not any(char.isdigit() for char in first) and any(char.isdigit() for char in second):
            reordered = ", ".join([second, first] + parts[2:])
            add(reordered)
            add(with_buenos_aires_context(reordered))

    if "Ciudad Autonoma de Buenos Aires" in base:
        short_variant = base.replace("Ciudad Autonoma de Buenos Aires", "Buenos Aires")
        add(short_variant)
        add(with_buenos_aires_context(short_variant))

    return candidates


def geocode_address(address: str) -> Optional[Tuple[float, float]]:
    for query in build_geocode_queries(address):
        params = urlencode({"q": query, "format": "json", "limit": 1})
        url = f"https://nominatim.openstreetmap.org/search?{params}"
        http_req = UrlRequest(
            url,
            headers={"User-Agent": "RutaExpressBA/1.0 (logistica local)"},
        )
        try:
            request = urlopen(http_req, timeout=8)
            content = request.read().decode("utf-8")
            rows = json.loads(content)
        except Exception:
            continue
        if not rows:
            continue

        try:
            lat = float(rows[0]["lat"])
            lng = float(rows[0]["lon"])
        except (KeyError, TypeError, ValueError):
            continue
        return (lat, lng)
    return None


def request_osrm_trip(coords: List[Tuple[float, float]]) -> Optional[Dict[str, object]]:
    if len(coords) < 2:
        return {"order": list(range(len(coords))), "path": [[lat, lng] for lat, lng in coords], "distance_km": 0.0, "duration_min": 0.0}

    coordinate_text = ";".join(f"{lng},{lat}" for lat, lng in coords)
    url = (
        "https://router.project-osrm.org/trip/v1/driving/"
        + coordinate_text
        + "?source=first&roundtrip=false&overview=full&geometries=geojson&steps=false"
    )

    try:
        request = UrlRequest(url, headers={"User-Agent": "RutaExpressBA/1.0 (trip-optimization)"})
        raw = urlopen(request, timeout=12).read().decode("utf-8")
        payload = json.loads(raw)
        trip = payload["trips"][0]
        waypoints = payload["waypoints"]
    except Exception:
        return None

    ordered_input_indexes = [int(item["waypoint_index"]) for item in sorted(waypoints, key=lambda item: item["trips_index"])]
    coords_geojson = trip["geometry"]["coordinates"]
    path = [[float(item[1]), float(item[0])] for item in coords_geojson]
    distance_km = round(float(trip.get("distance", 0.0)) / 1000.0, 2)
    duration_min = round(float(trip.get("duration", 0.0)) / 60.0, 1)
    return {
        "order": ordered_input_indexes,
        "path": path,
        "distance_km": distance_km,
        "duration_min": duration_min,
    }


def build_straight_route(coords: List[Tuple[float, float]]) -> Dict[str, object]:
    if len(coords) < 2:
        return {"path": [[lat, lng] for lat, lng in coords], "distance_km": 0.0, "duration_min": 0.0}

    total_km = 0.0
    for idx in range(1, len(coords)):
        total_km += haversine_km(
            coords[idx - 1][0], coords[idx - 1][1], coords[idx][0], coords[idx][1]
        )

    # Velocidad promedio simple para estimar.
    duration_min = (total_km / 30.0) * 60.0 if total_km > 0 else 0.0
    return {
        "path": [[lat, lng] for lat, lng in coords],
        "distance_km": round(total_km, 2),
        "duration_min": round(duration_min, 1),
    }


def request_osrm_route(coords: List[Tuple[float, float]]) -> Optional[Dict[str, object]]:
    if len(coords) < 2:
        return {"path": [[lat, lng] for lat, lng in coords], "distance_km": 0.0, "duration_min": 0.0}

    coordinate_text = ";".join(f"{lng},{lat}" for lat, lng in coords)
    url = (
        "https://router.project-osrm.org/route/v1/driving/"
        + coordinate_text
        + "?overview=full&geometries=geojson&steps=false"
    )

    try:
        request = UrlRequest(url, headers={"User-Agent": "RutaExpressBA/1.0 (routing)"})
        raw = urlopen(request, timeout=12).read().decode("utf-8")
        payload = json.loads(raw)
        route = payload["routes"][0]
        coords_geojson = route["geometry"]["coordinates"]
    except Exception:
        return None

    path = [[float(item[1]), float(item[0])] for item in coords_geojson]
    distance_km = round(float(route.get("distance", 0.0)) / 1000.0, 2)
    duration_min = round(float(route.get("duration", 0.0)) / 60.0, 1)
    return {"path": path, "distance_km": distance_km, "duration_min": duration_min}


def build_batched_road_route(coords: List[Tuple[float, float]]) -> Dict[str, object]:
    if len(coords) < 2:
        return build_straight_route(coords)

    max_points = 25
    stitched_path: List[List[float]] = []
    total_km = 0.0
    total_min = 0.0
    start = 0

    while start < len(coords) - 1:
        end = min(start + max_points - 1, len(coords) - 1)
        segment_coords = coords[start : end + 1]
        road = request_osrm_route(segment_coords)
        if road is None:
            road = build_straight_route(segment_coords)

        segment_path = road["path"]  # type: ignore[index]
        if stitched_path and segment_path:
            if stitched_path[-1] == segment_path[0]:
                stitched_path.extend(segment_path[1:])
            else:
                stitched_path.extend(segment_path)
        else:
            stitched_path.extend(segment_path)

        total_km += float(road["distance_km"])  # type: ignore[index]
        total_min += float(road["duration_min"])  # type: ignore[index]
        start = end

    return {
        "path": stitched_path,
        "distance_km": round(total_km, 2),
        "duration_min": round(total_min, 1),
    }


def resolve_route_plan(
    conn: sqlite3.Connection,
    conductor_id: int,
    fecha_filtro: str,
    estado_normalizado: str,
    origen_lat_value: float,
    origen_lng_value: float,
) -> Dict[str, object]:
    filtros = ["conductor_id = ?", "fecha = ?"]
    params: list[object] = [conductor_id, fecha_filtro]
    if estado_normalizado in ESTADOS_VALIDOS:
        filtros.append("estado = ?")
        params.append(estado_normalizado)

    sql = """
        SELECT id, cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, entrega_lat, entrega_lng, en_ruta_at, entregado_at
        FROM envios
    """
    sql += " WHERE " + " AND ".join(filtros)
    sql += " ORDER BY id DESC"
    envios = conn.execute(sql, params).fetchall()

    unresolved = []
    points = []
    for envio in envios:
        lat = envio["entrega_lat"]
        lng = envio["entrega_lng"]
        if lat is None or lng is None:
            coords = geocode_address(envio["direccion_entrega"])
            if coords:
                lat, lng = coords
                conn.execute(
                    "UPDATE envios SET entrega_lat = ?, entrega_lng = ? WHERE id = ?",
                    (lat, lng, envio["id"]),
                )
            else:
                unresolved.append(envio["direccion_entrega"])
                continue

        points.append(
            {
                "id": envio["id"],
                "cliente": envio["cliente"],
                "telefono": envio["telefono"],
                "direccion_retiro": envio["direccion_retiro"],
                "direccion_entrega": envio["direccion_entrega"],
                "estado": envio["estado"],
                "fecha": envio["fecha"],
                "en_ruta_at": envio["en_ruta_at"],
                "entregado_at": envio["entregado_at"],
                "lat": float(lat),
                "lng": float(lng),
            }
        )

    route_path: List[List[float]] = [[origen_lat_value, origen_lng_value]]
    total_km = 0.0
    total_min = 0.0

    if points:
        trip_coords = [(origen_lat_value, origen_lng_value)] + [
            (float(point["lat"]), float(point["lng"])) for point in points
        ]
        optimized_trip = request_osrm_trip(trip_coords)

        if optimized_trip is not None:
            ordered_indexes = [
                idx - 1 for idx in optimized_trip["order"] if int(idx) > 0  # type: ignore[index]
            ]
            ordered = [points[idx] for idx in ordered_indexes if 0 <= idx < len(points)]
            if len(ordered) == len(points):
                route_path = optimized_trip["path"]  # type: ignore[index]
                total_km = float(optimized_trip["distance_km"])  # type: ignore[index]
                total_min = float(optimized_trip["duration_min"])  # type: ignore[index]
            else:
                optimized_trip = None

        if not points:
            ordered = []
        elif optimized_trip is None:
            ordered = optimize_order(points, origen_lat_value, origen_lng_value)
            road_coords = [(origen_lat_value, origen_lng_value)] + [
                (float(stop["lat"]), float(stop["lng"])) for stop in ordered
            ]
            road_route = build_batched_road_route(road_coords)
            route_path = road_route["path"]
            total_km = float(road_route["distance_km"])
            total_min = float(road_route["duration_min"])
    else:
        ordered = []

    prev_lat = origen_lat_value
    prev_lng = origen_lng_value
    for stop in ordered:
        hop = haversine_km(prev_lat, prev_lng, stop["lat"], stop["lng"])
        stop["km_desde_anterior"] = round(hop, 2)
        prev_lat = stop["lat"]
        prev_lng = stop["lng"]

    return {
        "ordered": ordered,
        "unresolved": unresolved,
        "route_path": route_path,
        "total_km": round(total_km, 2),
        "total_min": round(total_min, 1),
    }


def init_db():
    conn = get_db()

    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS users(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE,
    password TEXT
    )
    """
    )

    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS conductores(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL,
    telefono TEXT
    )
    """
    )

    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS envios(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cliente TEXT,
    telefono TEXT,
    direccion_retiro TEXT,
    direccion_entrega TEXT,
    fecha TEXT,
    entrega_lat REAL,
    entrega_lng REAL,
    estado TEXT,
    conductor_id INTEGER
    )
    """
    )

    columnas_envios = [
        row["name"] for row in conn.execute("PRAGMA table_info(envios)").fetchall()
    ]
    if "conductor_id" not in columnas_envios:
        conn.execute("ALTER TABLE envios ADD COLUMN conductor_id INTEGER")
    if "fecha" not in columnas_envios:
        conn.execute("ALTER TABLE envios ADD COLUMN fecha TEXT")
    if "entrega_lat" not in columnas_envios:
        conn.execute("ALTER TABLE envios ADD COLUMN entrega_lat REAL")
    if "entrega_lng" not in columnas_envios:
        conn.execute("ALTER TABLE envios ADD COLUMN entrega_lng REAL")
    if "en_ruta_at" not in columnas_envios:
        conn.execute("ALTER TABLE envios ADD COLUMN en_ruta_at TEXT")
    if "entregado_at" not in columnas_envios:
        conn.execute("ALTER TABLE envios ADD COLUMN entregado_at TEXT")

    conn.execute(
        "UPDATE envios SET fecha = ? WHERE fecha IS NULL OR fecha = ''",
        (today_iso(),),
    )

    conteo_conductores = conn.execute("SELECT COUNT(*) FROM conductores").fetchone()[0]
    if conteo_conductores == 0:
        conn.execute(
            "INSERT INTO conductores (nombre, telefono) VALUES (?, ?)",
            ("Franco Diaz", "+54 11 2456 0099"),
        )
        conn.execute(
            "INSERT INTO conductores (nombre, telefono) VALUES (?, ?)",
            ("Lucia Benitez", "+54 11 3987 1140"),
        )

    conteo_usuarios = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if conteo_usuarios == 0:
        conn.execute(
            "INSERT INTO users (email, password) VALUES (?, ?)",
            ("admin@rutaexpress.local", hash_password("admin1234")),
        )

    conn.commit()
    conn.close()


init_db()


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if request.session.get("user_id"):
        return RedirectResponse("/dashboard", status_code=303)
    return templates.TemplateResponse(request, "login.html")


@app.post("/login")
def login(request: Request, email: str = Form(...), password: str = Form(...)):
    conn = get_db()
    user = conn.execute(
        "SELECT id, email, password FROM users WHERE email = ?",
        (email,),
    ).fetchone()

    if not user or not verify_password(password, user["password"]):
        conn.close()
        return RedirectResponse("/login", status_code=303)

    if not user["password"].startswith("pbkdf2_sha256$"):
        conn.execute(
            "UPDATE users SET password = ? WHERE id = ?",
            (hash_password(password), user["id"]),
        )
        conn.commit()

    conn.close()
    request.session["user_id"] = user["id"]
    request.session["user_email"] = user["email"]
    return RedirectResponse("/dashboard", status_code=303)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    estado: str = "",
    q: str = "",
    conductor: str = "",
    fecha: str = "",
    desde: str = "",
    hasta: str = "",
):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    conn = get_db()
    filtros = []
    params = []

    estado_normalizado = estado.strip().lower().replace(" ", "_")
    query_texto = q.strip()
    fecha_filtro = normalize_optional_date(fecha)
    desde_filtro = normalize_optional_date(desde)
    hasta_filtro = normalize_optional_date(hasta)

    conductor_id = None
    if conductor.strip().isdigit():
        conductor_id = int(conductor.strip())

    if fecha_filtro:
        filtros.append("e.fecha = ?")
        params.append(fecha_filtro)
    else:
        if desde_filtro:
            filtros.append("e.fecha >= ?")
            params.append(desde_filtro)
        if hasta_filtro:
            filtros.append("e.fecha <= ?")
            params.append(hasta_filtro)

    if estado_normalizado in ESTADOS_VALIDOS:
        filtros.append("e.estado = ?")
        params.append(estado_normalizado)

    if query_texto:
        filtros.append("(e.cliente LIKE ? OR e.telefono LIKE ?)")
        like_val = f"%{query_texto}%"
        params.extend([like_val, like_val])

    if conductor_id:
        filtros.append("e.conductor_id = ?")
        params.append(conductor_id)

    sql = """
        SELECT e.*, c.nombre AS conductor_nombre
        FROM envios e
        LEFT JOIN conductores c ON c.id = e.conductor_id
    """
    if filtros:
        sql += " WHERE " + " AND ".join(filtros)
    sql += " ORDER BY e.id DESC"

    envios = conn.execute(sql, params).fetchall()
    conductores = conn.execute(
        "SELECT id, nombre FROM conductores ORDER BY nombre ASC"
    ).fetchall()
    conn.close()

    total_envios = len(envios)
    total_entregados = sum(1 for envio in envios if envio["estado"] == "entregado")
    total_pendientes = sum(1 for envio in envios if envio["estado"] == "pendiente")
    total_en_ruta = sum(1 for envio in envios if envio["estado"] == "en_ruta")
    conductores_activos = len(
        {envio["conductor_id"] for envio in envios if envio["conductor_id"] is not None}
    )
    delivery_minutes = [
        value
        for value in (
            calculate_delivery_minutes(envio["en_ruta_at"], envio["entregado_at"])
            for envio in envios
        )
        if value is not None
    ]
    promedio_entrega_min = round(sum(delivery_minutes) / len(delivery_minutes), 1) if delivery_minutes else None

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "envios": envios,
            "conductores": conductores,
            "filtro_estado": estado_normalizado if estado_normalizado in ESTADOS_VALIDOS else "",
            "filtro_q": query_texto,
            "filtro_conductor": conductor_id,
            "filtro_fecha": fecha_filtro,
            "filtro_desde": desde_filtro,
            "filtro_hasta": hasta_filtro,
            "usuario_email": request.session.get("user_email", ""),
            "metricas": {
                "total_envios": total_envios,
                "total_entregados": total_entregados,
                "total_pendientes": total_pendientes,
                "total_en_ruta": total_en_ruta,
                "conductores_activos": conductores_activos,
                "promedio_entrega_min": promedio_entrega_min,
            },
        },
    )


@app.get("/dashboard/hoja-ruta.csv")
def descargar_hoja_ruta(
    request: Request,
    estado: str = "",
    q: str = "",
    conductor: str = "",
    fecha: str = "",
    desde: str = "",
    hasta: str = "",
):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    conn = get_db()
    filtros = []
    params = []

    estado_normalizado = estado.strip().lower().replace(" ", "_")
    query_texto = q.strip()
    fecha_filtro = normalize_optional_date(fecha)
    desde_filtro = normalize_optional_date(desde)
    hasta_filtro = normalize_optional_date(hasta)

    conductor_id = None
    if conductor.strip().isdigit():
        conductor_id = int(conductor.strip())

    if fecha_filtro:
        filtros.append("e.fecha = ?")
        params.append(fecha_filtro)
    else:
        if desde_filtro:
            filtros.append("e.fecha >= ?")
            params.append(desde_filtro)
        if hasta_filtro:
            filtros.append("e.fecha <= ?")
            params.append(hasta_filtro)

    if estado_normalizado in ESTADOS_VALIDOS:
        filtros.append("e.estado = ?")
        params.append(estado_normalizado)

    if query_texto:
        filtros.append("(e.cliente LIKE ? OR e.telefono LIKE ?)")
        like_val = f"%{query_texto}%"
        params.extend([like_val, like_val])

    if conductor_id:
        filtros.append("e.conductor_id = ?")
        params.append(conductor_id)

    sql = """
        SELECT e.id, e.fecha, e.cliente, e.telefono, e.direccion_retiro, e.direccion_entrega, e.estado, c.nombre AS conductor_nombre
        FROM envios e
        LEFT JOIN conductores c ON c.id = e.conductor_id
    """
    if filtros:
        sql += " WHERE " + " AND ".join(filtros)
    sql += " ORDER BY e.id DESC"

    envios = conn.execute(sql, params).fetchall()
    conductor_nombre = "todos"
    if conductor_id:
        conductor_row = conn.execute(
            "SELECT nombre FROM conductores WHERE id = ?", (conductor_id,)
        ).fetchone()
        if conductor_row:
            conductor_nombre = conductor_row["nombre"].strip().replace(" ", "_").lower()
    conn.close()

    buffer = io.StringIO()
    csv_writer = writer(buffer, delimiter=";")
    csv_writer.writerow(
        [
            "id",
            "fecha",
            "cliente",
            "telefono",
            "direccion_retiro",
            "direccion_entrega",
            "conductor",
            "estado",
        ]
    )
    for envio in envios:
        csv_writer.writerow(
            [
                envio["id"],
                envio["fecha"],
                envio["cliente"],
                envio["telefono"],
                envio["direccion_retiro"],
                envio["direccion_entrega"],
                envio["conductor_nombre"] or "",
                envio["estado"],
            ]
        )

    csv_content = "\ufeff" + buffer.getvalue()
    filename_date = fecha_filtro or (
        f"{desde_filtro or 'inicio'}_a_{hasta_filtro or 'fin'}"
        if desde_filtro or hasta_filtro
        else "todas_las_fechas"
    )
    filename = f"hoja_ruta_{filename_date}_{conductor_nombre}.csv"
    return Response(
        content=csv_content,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/conductores", response_class=HTMLResponse)
def conductores_page(request: Request):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    conn = get_db()
    conductores = conn.execute(
        "SELECT id, nombre, telefono FROM conductores ORDER BY nombre ASC"
    ).fetchall()
    conn.close()

    return templates.TemplateResponse(
        request,
        "conductores.html",
        {
            "conductores": conductores,
            "usuario_email": request.session.get("user_email", ""),
        },
    )


@app.post("/conductores")
def crear_conductor(request: Request, nombre: str = Form(...), telefono: str = Form("")):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    nombre_normalizado = nombre.strip()
    telefono_normalizado = telefono.strip()
    if not nombre_normalizado:
        return RedirectResponse("/conductores", status_code=303)

    conn = get_db()
    conn.execute(
        "INSERT INTO conductores (nombre, telefono) VALUES (?, ?)",
        (nombre_normalizado, telefono_normalizado),
    )
    conn.commit()
    conn.close()

    return RedirectResponse("/conductores", status_code=303)


@app.post("/conductores/{conductor_id}/editar")
def editar_conductor(
    request: Request,
    conductor_id: int,
    nombre: str = Form(...),
    telefono: str = Form(""),
):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    nombre_normalizado = nombre.strip()
    telefono_normalizado = telefono.strip()
    if not nombre_normalizado:
        return RedirectResponse("/conductores", status_code=303)

    conn = get_db()
    conn.execute(
        "UPDATE conductores SET nombre = ?, telefono = ? WHERE id = ?",
        (nombre_normalizado, telefono_normalizado, conductor_id),
    )
    conn.commit()
    conn.close()

    return RedirectResponse("/conductores", status_code=303)


@app.post("/conductores/{conductor_id}/eliminar")
def eliminar_conductor(request: Request, conductor_id: int):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    conn = get_db()
    conn.execute("UPDATE envios SET conductor_id = NULL WHERE conductor_id = ?", (conductor_id,))
    conn.execute("DELETE FROM conductores WHERE id = ?", (conductor_id,))
    conn.commit()
    conn.close()

    return RedirectResponse("/conductores", status_code=303)


@app.get("/conductores/{conductor_id}/ruta", response_class=HTMLResponse)
def ruta_conductor(
    request: Request,
    conductor_id: int,
    fecha: str = "",
    estado: str = "",
    origen_lat: str = "",
    origen_lng: str = "",
    auto_waze: str = "",
):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    fecha_filtro = normalize_date_or_today(fecha)
    estado_normalizado = estado.strip().lower().replace(" ", "_")

    conn = get_db()
    conductor = conn.execute(
        "SELECT id, nombre, telefono FROM conductores WHERE id = ?", (conductor_id,)
    ).fetchone()
    if not conductor:
        conn.close()
        return RedirectResponse("/dashboard", status_code=303)

    origen_lat_value = parse_coordinate(origen_lat, -90.0, 90.0)
    origen_lng_value = parse_coordinate(origen_lng, -180.0, 180.0)
    if origen_lat_value is None or origen_lng_value is None:
        origen_lat_value = -34.6037
        origen_lng_value = -58.3816

    origin_label = "Ubicacion actual" if origen_lat_value != -34.6037 or origen_lng_value != -58.3816 else "Origen sugerido (centro CABA)"
    route_plan = resolve_route_plan(
        conn,
        conductor_id,
        fecha_filtro,
        estado_normalizado,
        origen_lat_value,
        origen_lng_value,
    )
    conn.commit()
    conn.close()

    return templates.TemplateResponse(
        request,
        "ruta_conductor.html",
        {
            "conductor": conductor,
            "fecha_filtro": fecha_filtro,
            "estado_filtro": estado_normalizado if estado_normalizado in ESTADOS_VALIDOS else "",
            "stops": route_plan["ordered"],
            "unresolved": route_plan["unresolved"],
            "total_km": route_plan["total_km"],
            "total_min": route_plan["total_min"],
            "route_path": route_plan["route_path"],
            "usuario_email": request.session.get("user_email", ""),
            "origen_lat": origen_lat_value,
            "origen_lng": origen_lng_value,
            "origin_label": origin_label,
            "auto_waze": auto_waze == "1",
        },
    )


def advance_conductor_route_status(
    conductor_id: int,
    fecha_filtro: str,
    origen_lat_value: float,
    origen_lng_value: float,
    complete_envio_id: Optional[int] = None,
) -> None:
    conn = get_db()
    route_plan = resolve_route_plan(
        conn,
        conductor_id,
        fecha_filtro,
        "",
        origen_lat_value,
        origen_lng_value,
    )
    ordered = route_plan["ordered"]  # type: ignore[assignment]
    ordered_ids = [int(stop["id"]) for stop in ordered]

    if complete_envio_id is not None:
        conn.execute(
            "UPDATE envios SET estado = 'entregado', entregado_at = ? WHERE id = ? AND conductor_id = ? AND fecha = ?",
            (current_timestamp(), complete_envio_id, conductor_id, fecha_filtro),
        )

    current_in_route = conn.execute(
        "SELECT id FROM envios WHERE conductor_id = ? AND fecha = ? AND estado = 'en_ruta' ORDER BY id ASC",
        (conductor_id, fecha_filtro),
    ).fetchone()
    if current_in_route:
        conn.execute(
            "UPDATE envios SET estado = 'pendiente' WHERE id = ?",
            (current_in_route["id"],),
        )

    next_id = None
    delivered_ids = {
        row["id"]
        for row in conn.execute(
            "SELECT id FROM envios WHERE conductor_id = ? AND fecha = ? AND estado = 'entregado'",
            (conductor_id, fecha_filtro),
        ).fetchall()
    }
    for envio_id in ordered_ids:
        if envio_id not in delivered_ids:
            next_id = envio_id
            break

    if next_id is not None:
        conn.execute(
            "UPDATE envios SET estado = 'en_ruta', en_ruta_at = COALESCE(en_ruta_at, ?) WHERE id = ?",
            (current_timestamp(), next_id),
        )

    conn.commit()
    conn.close()


@app.post("/conductores/{conductor_id}/ruta/iniciar")
def iniciar_ruta_conductor(
    request: Request,
    conductor_id: int,
    fecha: str = Form(""),
    origen_lat: str = Form(""),
    origen_lng: str = Form(""),
):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    fecha_filtro = normalize_date_or_today(fecha)
    origen_lat_value = parse_coordinate(origen_lat, -90.0, 90.0) or -34.6037
    origen_lng_value = parse_coordinate(origen_lng, -180.0, 180.0) or -58.3816
    advance_conductor_route_status(conductor_id, fecha_filtro, origen_lat_value, origen_lng_value)
    redirect_query = urlencode(
        {
            "fecha": fecha_filtro,
            "origen_lat": origen_lat_value,
            "origen_lng": origen_lng_value,
            "auto_waze": 1,
        }
    )
    return RedirectResponse(f"/conductores/{conductor_id}/ruta?{redirect_query}", status_code=303)


@app.post("/conductores/{conductor_id}/ruta/{envio_id}/entregado")
def entregar_y_avanzar_ruta_conductor(
    request: Request,
    conductor_id: int,
    envio_id: int,
    fecha: str = Form(""),
    origen_lat: str = Form(""),
    origen_lng: str = Form(""),
):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    fecha_filtro = normalize_date_or_today(fecha)
    origen_lat_value = parse_coordinate(origen_lat, -90.0, 90.0) or -34.6037
    origen_lng_value = parse_coordinate(origen_lng, -180.0, 180.0) or -58.3816
    advance_conductor_route_status(
        conductor_id,
        fecha_filtro,
        origen_lat_value,
        origen_lng_value,
        complete_envio_id=envio_id,
    )
    redirect_query = urlencode(
        {
            "fecha": fecha_filtro,
            "origen_lat": origen_lat_value,
            "origen_lng": origen_lng_value,
            "auto_waze": 1,
        }
    )
    return RedirectResponse(f"/conductores/{conductor_id}/ruta?{redirect_query}", status_code=303)


@app.get("/nuevo-envio", response_class=HTMLResponse)
def nuevo_envio_page(request: Request):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    conn = get_db()
    conductores = conn.execute(
        "SELECT id, nombre FROM conductores ORDER BY nombre ASC"
    ).fetchall()
    conn.close()

    return templates.TemplateResponse(
        request,
        "nuevo_envio.html",
        {
            "conductores": conductores,
            "fecha_hoy": today_iso(),
            "usuario_email": request.session.get("user_email", ""),
        },
    )


@app.post("/nuevo-envio")
def nuevo_envio(
    request: Request,
    cliente: str = Form(...),
    telefono: str = Form(...),
    direccion_retiro: str = Form(""),
    direccion_entrega: str = Form(...),
    fecha: str = Form(""),
    conductor_id: int = Form(...),
):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    fecha_envio = normalize_date_or_today(fecha)

    conn = get_db()
    conductor = conn.execute(
        "SELECT id FROM conductores WHERE id = ?", (conductor_id,)
    ).fetchone()
    conductor_id_val = conductor["id"] if conductor else None

    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, fecha, estado, conductor_id)
        VALUES (?, ?, ?, ?, ?, 'pendiente', ?)
        """,
        (
            cliente,
            telefono,
            direccion_retiro.strip(),
            direccion_entrega,
            fecha_envio,
            conductor_id_val,
        ),
    )

    conn.commit()
    conn.close()
    redirect_query = urlencode({"fecha": fecha_envio})
    return RedirectResponse(f"/dashboard?{redirect_query}", status_code=303)


@app.post("/envio/{envio_id}/estado")
def actualizar_estado(request: Request, envio_id: int, estado: str = Form(...)):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    estado_normalizado = estado.strip().lower().replace(" ", "_")

    if estado_normalizado not in ESTADOS_VALIDOS:
        return RedirectResponse("/dashboard", status_code=303)

    conn = get_db()
    conn.execute(
        "UPDATE envios SET estado = ? WHERE id = ?",
        (estado_normalizado, envio_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/dashboard", status_code=303)


@app.get("/acceso-conductor", response_class=HTMLResponse)
def acceso_conductor_page(request: Request):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    return RedirectResponse("/dashboard", status_code=303)


@app.post("/acceso-conductor")
def acceso_conductor(request: Request, conductor_id: int = Form(...)):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    return RedirectResponse(f"/dashboard?conductor={conductor_id}", status_code=303)


@app.get("/conductor/{conductor_id}", response_class=HTMLResponse)
def panel_conductor(request: Request, conductor_id: int):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    return RedirectResponse(f"/dashboard?conductor={conductor_id}", status_code=303)


@app.post("/conductor/{conductor_id}/envio/{envio_id}/estado")
def actualizar_estado_conductor(
    request: Request,
    conductor_id: int,
    envio_id: int,
    estado: str = Form(...),
):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    estado_normalizado = estado.strip().lower().replace(" ", "_")

    if estado_normalizado not in ESTADOS_VALIDOS:
        return RedirectResponse(f"/dashboard?conductor={conductor_id}", status_code=303)

    conn = get_db()
    conn.execute(
        "UPDATE envios SET estado = ? WHERE id = ? AND conductor_id = ?",
        (estado_normalizado, envio_id, conductor_id),
    )
    conn.commit()
    conn.close()

    return RedirectResponse(f"/dashboard?conductor={conductor_id}", status_code=303)
