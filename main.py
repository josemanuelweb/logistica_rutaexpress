import hashlib
import hmac
import os
import secrets
import sqlite3

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
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
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if request.session.get("user_id"):
        return RedirectResponse("/dashboard", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request})


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
def dashboard(request: Request, estado: str = "", q: str = "", conductor: str = ""):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    conn = get_db()
    filtros = []
    params = []

    estado_normalizado = estado.strip().lower().replace(" ", "_")
    query_texto = q.strip()

    conductor_id = None
    if conductor.strip().isdigit():
        conductor_id = int(conductor.strip())

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

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "envios": envios,
            "conductores": conductores,
            "filtro_estado": estado_normalizado if estado_normalizado in ESTADOS_VALIDOS else "",
            "filtro_q": query_texto,
            "filtro_conductor": conductor_id,
            "usuario_email": request.session.get("user_email", ""),
        },
    )


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
        "nuevo_envio.html",
        {
            "request": request,
            "conductores": conductores,
            "usuario_email": request.session.get("user_email", ""),
        },
    )


@app.post("/nuevo-envio")
def nuevo_envio(
    request: Request,
    cliente: str = Form(...),
    telefono: str = Form(...),
    direccion_retiro: str = Form(...),
    direccion_entrega: str = Form(...),
    conductor_id: int = Form(...),
):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    conn = get_db()
    conductor = conn.execute(
        "SELECT id FROM conductores WHERE id = ?", (conductor_id,)
    ).fetchone()
    conductor_id_val = conductor["id"] if conductor else None

    conn.execute(
        """
        INSERT INTO envios
        (cliente, telefono, direccion_retiro, direccion_entrega, estado, conductor_id)
        VALUES (?, ?, ?, ?, 'pendiente', ?)
        """,
        (cliente, telefono, direccion_retiro, direccion_entrega, conductor_id_val),
    )

    conn.commit()
    conn.close()
    return RedirectResponse("/dashboard", status_code=303)


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

    conn = get_db()
    conductores = conn.execute(
        "SELECT id, nombre, telefono FROM conductores ORDER BY nombre ASC"
    ).fetchall()
    conn.close()

    return templates.TemplateResponse(
        "conductor_acceso.html",
        {
            "request": request,
            "conductores": conductores,
            "usuario_email": request.session.get("user_email", ""),
        },
    )


@app.post("/acceso-conductor")
def acceso_conductor(request: Request, conductor_id: int = Form(...)):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    return RedirectResponse(f"/conductor/{conductor_id}", status_code=303)


@app.get("/conductor/{conductor_id}", response_class=HTMLResponse)
def panel_conductor(request: Request, conductor_id: int):
    auth_redirect = login_required(request)
    if auth_redirect:
        return auth_redirect

    conn = get_db()
    conductor = conn.execute(
        "SELECT id, nombre, telefono FROM conductores WHERE id = ?", (conductor_id,)
    ).fetchone()

    if not conductor:
        conn.close()
        return RedirectResponse("/acceso-conductor", status_code=303)

    envios = conn.execute(
        """
        SELECT *
        FROM envios
        WHERE conductor_id = ?
        ORDER BY CASE estado
            WHEN 'pendiente' THEN 1
            WHEN 'en_ruta' THEN 2
            WHEN 'entregado' THEN 3
            ELSE 4
        END, id DESC
        """,
        (conductor_id,),
    ).fetchall()
    conn.close()

    return templates.TemplateResponse(
        "conductor_panel.html",
        {
            "request": request,
            "conductor": conductor,
            "envios": envios,
            "usuario_email": request.session.get("user_email", ""),
        },
    )


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
        return RedirectResponse(f"/conductor/{conductor_id}", status_code=303)

    conn = get_db()
    conn.execute(
        "UPDATE envios SET estado = ? WHERE id = ? AND conductor_id = ?",
        (estado_normalizado, envio_id, conductor_id),
    )
    conn.commit()
    conn.close()

    return RedirectResponse(f"/conductor/{conductor_id}", status_code=303)
