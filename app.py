from flask import Flask, jsonify, send_from_directory
import json
import os

app = Flask(__name__)

@app.route("/")
def inicio():
    return send_from_directory(".", "index.html")


@app.route("/api/alertas")
def obtener_alertas():

    archivo = "alertas.json"

    if not os.path.exists(archivo):
        return jsonify([])

    try:

        with open(
            archivo,
            "r",
            encoding="utf-8"
        ) as f:

            datos = json.load(f)

        return jsonify(datos)

    except Exception as e:

        return jsonify({
            "error": str(e)
        })

@app.route("/api/resumen")
def obtener_resumen():

    try:

        with open(
            "alertas.json",
            "r",
            encoding="utf-8"
        ) as archivo:

            alertas = json.load(archivo)

    except Exception:

        alertas = []

    total_alertas = len(alertas)

    riesgo_alto = len([
        a for a in alertas
        if a["riesgo"] == "ALTO"
    ])

    riesgo_medio = len([
        a for a in alertas
        if a["riesgo"] == "MEDIO"
    ])

    riesgo_bajo = len([
        a for a in alertas
        if a["riesgo"] == "BAJO"
    ])

    return jsonify({
        "total_alertas": total_alertas,
        "riesgo_alto": riesgo_alto,
        "riesgo_medio": riesgo_medio,
        "riesgo_bajo": riesgo_bajo
    })

if __name__ == "__main__":

    app.run(
        host="127.0.0.1",
        port=5000,
        debug=True
    )