# pyrefly: ignore[missing-import]
from flask import Flask, jsonify, send_from_directory
import os
import database

app = Flask(__name__)

# Asegurar que la base de datos esté inicializada al arrancar la app
database.inicializar_db()
database.migrar_desde_json()


@app.route("/")
def inicio():
    return send_from_directory(".", "index.html")


@app.route("/api/alertas")
def obtener_alertas_endpoint():
    try:
        alertas = database.obtener_alertas(limite=100)
        return jsonify(alertas)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/resumen")
def obtener_resumen_endpoint():
    try:
        resumen = database.obtener_resumen()
        return jsonify(resumen)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=5000,
        debug=True
    )