import sqlite3
import json
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "alertas.db")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


def obtener_conexion():
    """Retorna una conexión a la base de datos SQLite."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def inicializar_db():
    """Crea las tablas a partir del archivo schema.sql si no existen."""
    with obtener_conexion() as conn:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            conn.executescript(f.read())
        conn.commit()


def guardar_alerta(persona, total, riesgo, fecha=None):
    """Inserta una alerta individual en la base de datos."""
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with obtener_conexion() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO alertas (persona, total, riesgo, fecha)
            VALUES (?, ?, ?, ?)
            """,
            (persona, total, riesgo, fecha)
        )
        conn.commit()
        return cursor.lastrowid


def guardar_alertas_lote(lista_alertas):
    """Guarda una lista de alertas en una sola transacción."""
    if not lista_alertas:
        return

    registros = [
        (
            a["persona"],
            int(a["total"]),
            a["riesgo"],
            a.get("fecha", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        for a in lista_alertas
    ]

    with obtener_conexion() as conn:
        conn.executemany(
            """
            INSERT INTO alertas (persona, total, riesgo, fecha)
            VALUES (?, ?, ?, ?)
            """,
            registros
        )
        conn.commit()


def obtener_alertas(limite=100):
    """Obtiene las alertas más recientes ordenadas por id descendente."""
    inicializar_db()
    with obtener_conexion() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, persona, total, riesgo, fecha
            FROM alertas
            ORDER BY id DESC
            LIMIT ?
            """,
            (limite,)
        )
        filas = cursor.fetchall()
        return [dict(f) for f in filas]


def obtener_resumen():
    """Calcula totales y desglose por riesgo directamente en SQL."""
    inicializar_db()
    with obtener_conexion() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_alertas,
                SUM(CASE WHEN riesgo = 'ALTO' THEN 1 ELSE 0 END) AS riesgo_alto,
                SUM(CASE WHEN riesgo = 'MEDIO' THEN 1 ELSE 0 END) AS riesgo_medio,
                SUM(CASE WHEN riesgo = 'BAJO' THEN 1 ELSE 0 END) AS riesgo_bajo
            FROM alertas
            """
        )
        fila = cursor.fetchone()
        if fila:
            return {
                "total_alertas": fila["total_alertas"] or 0,
                "riesgo_alto": fila["riesgo_alto"] or 0,
                "riesgo_medio": fila["riesgo_medio"] or 0,
                "riesgo_bajo": fila["riesgo_bajo"] or 0
            }
        return {
            "total_alertas": 0,
            "riesgo_alto": 0,
            "riesgo_medio": 0,
            "riesgo_bajo": 0
        }


def migrar_desde_json(archivo_json="alertas.json"):
    """Si existe alertas.json, importa su historial a la base de datos."""
    if not os.path.exists(archivo_json):
        return 0

    inicializar_db()

    with open(archivo_json, "r", encoding="utf-8") as f:
        try:
            datos = json.load(f)
        except Exception:
            datos = []

    if not datos:
        return 0

    with obtener_conexion() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS total FROM alertas")
        total_actual = cursor.fetchone()["total"]

        # Si ya hay registros, evitamos duplicar la migración inicial
        if total_actual > 0:
            return 0

        registros = [
            (
                d.get("persona", "Desconocido"),
                int(d.get("total", 0)),
                d.get("riesgo", "BAJO"),
                d.get("fecha", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )
            for d in datos
        ]

        cursor.executemany(
            """
            INSERT INTO alertas (persona, total, riesgo, fecha)
            VALUES (?, ?, ?, ?)
            """,
            registros
        )
        conn.commit()
        return len(registros)


if __name__ == "__main__":
    print("Inicializando base de datos SQLite...")
    inicializar_db()
    migrados = migrar_desde_json()
    print(f"Base de datos lista en: {DB_PATH}")
    if migrados > 0:
        print(f"Se migraron {migrados} alertas existentes desde alertas.json.")
    print("Resumen actual:", obtener_resumen())
