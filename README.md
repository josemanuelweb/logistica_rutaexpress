# RutaExpress

Aplicacion web de logistica para gestionar envios, conductores y hojas de ruta.

## Stack

- FastAPI
- Jinja2
- SQLite
- Pytest

## Funcionalidades

- Login con sesion para operadores
- Alta y administracion de conductores
- Alta de envios con asignacion de conductor
- Panel de envios con control de estados
- Vista operativa para conductor
- Generacion de hoja de ruta en CSV
- Calculo y visualizacion de rutas usando geocodificacion y OSRM cuando esta disponible

## Requisitos

- Python 3.11 o superior

## Instalacion

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Ejecucion local

```bash
export SESSION_SECRET="cambia-esta-clave"
uvicorn main:app --reload
```

La aplicacion queda disponible en `http://127.0.0.1:8000`.

## Variables de entorno

- `DATABASE_PATH`: ruta del archivo SQLite. Por defecto `database.db`
- `SESSION_SECRET`: clave de sesion para FastAPI/Starlette

## Testing

```bash
pytest
```

## Rutas principales

- `/login`
- `/dashboard`
- `/dashboard/hoja-ruta.csv`
- `/conductores`
- `/nuevo-envio`
- `/acceso-conductor`

## Notas

- La base de datos se inicializa automaticamente al arrancar la aplicacion.
- Si los servicios externos de geocodificacion o ruteo no responden, la aplicacion usa una estimacion alternativa para mantener la operacion.
