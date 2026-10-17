"""
Segundo productor: simula transacciones más realistas.

- Envía ráfagas (varias transacciones seguidas) para simular
  actividad sospechosa.
- Incluye montos grandes ocasionales para disparar el detector.
- Usa el mismo tema 'transacciones' que el primer productor.
- Los IDs llevan prefijo 'B' para distinguirlos del productor 1.
"""

import json
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

SERVIDOR = "localhost:9092"
TEMA = "transacciones"

# Personas (puedes cambiarlas o añadir más)
PERSONAS = ["Ana", "Carlos", "Laura", "Pedro"]

# Perfiles: cuánto y con qué frecuencia gasta cada uno
PERFILES = {
    "Ana":    {"min": 20000, "max": 60000,  "peso_fraude": 0.15},
    "Carlos": {"min": 30000, "max": 90000,  "peso_fraude": 0.25},
    "Laura":  {"min": 15000, "max": 50000,  "peso_fraude": 0.10},
    "Pedro":  {"min": 40000, "max": 120000, "peso_fraude": 0.35},
}


def conectar():
    """Reintenta la conexión a Kafka hasta lograrlo."""
    while True:
        try:
            productor = KafkaProducer(
                bootstrap_servers=SERVIDOR,
                value_serializer=lambda dato: json.dumps(dato).encode("utf-8"),
                linger_ms=10,
                acks="all",
            )
            print("[Productor2] Conectado a Kafka.")
            return productor
        except NoBrokersAvailable:
            print("[Productor2] Kafka no disponible, reintentando en 3s...")
            time.sleep(3)


def generar_transaccion(persona):
    """Genera una transacción según el perfil de la persona."""
    perfil = PERFILES[persona]

    # 20% de las veces forzamos un monto alto (simula fraude)
    if random.random() < perfil["peso_fraude"]:
        monto = random.randint(200_000, 900_000)
        tipo = "SOSPECHOSA"
    else:
        monto = random.randint(perfil["min"], perfil["max"])
        tipo = "normal"

    return {
        "id": "B" + str(uuid.uuid4())[:7],
        "persona": persona,
        "monto": monto,
        "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }, tipo


def enviar_rafaga(productor, persona, cantidad):
    """Envía varias transacciones seguidas para simular una ráfaga."""
    print(f"[Productor2] Ráfaga de {cantidad} para {persona}")
    for _ in range(cantidad):
        transaccion, tipo = generar_transaccion(persona)
        productor.send(TEMA, transaccion)
        print(f"  [{tipo}] {transaccion['persona']} "
              f"${transaccion['monto']:,} id={transaccion['id']}")
        time.sleep(random.uniform(0.1, 0.4))
    productor.flush()


def main():
    productor = conectar()

    try:
        while True:
            persona = random.choice(PERSONAS)

            # 30% de las veces mandamos ráfaga (simula ataque)
            if random.random() < 0.30:
                cantidad = random.randint(3, 7)
                enviar_rafaga(productor, persona, cantidad)
            else:
                transaccion, tipo = generar_transaccion(persona)
                productor.send(TEMA, transaccion)
                productor.flush()
                print(f"[Productor2] [{tipo}] {transaccion['persona']} "
                      f"${transaccion['monto']:,} id={transaccion['id']}")

            # Espera variable entre envíos
            time.sleep(random.uniform(1.5, 4.0))

    except KeyboardInterrupt:
        print("\n[Productor2] Detenido por el usuario.")
    finally:
        productor.close()


if __name__ == "__main__":
    main()