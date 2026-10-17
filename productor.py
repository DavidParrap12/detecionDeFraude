import json
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer
from kafka import errors

servidor = "localhost:9092"
tema = "transacciones"

while True:
    try:
        productor = KafkaProducer(
            bootstrap_servers=servidor,
            value_serializer=lambda dato: json.dumps(dato).encode("utf-8")
        )
        break
    except Exception as e:
        print("Esperando a que Kafka esté disponible...")
        print(e)
        time.sleep(3)

personas = ["Ana", "Carlos", "Laura", "Pedro"]

print("Productor conectado a Kafka.")

try:
    while True:
        transaccion = {
            "id": str(uuid.uuid4())[:8],
            "persona": random.choice(personas),
            "monto": random.randint(10000, 50000),
            "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        future = productor.send(tema, transaccion)

        metadata = future.get(timeout=10)

        print(
            f"Mensaje enviado a {metadata.topic}",
            flush=True
        )

        productor.flush()

        print("Transacción enviada:", transaccion, flush=True)
        time.sleep(2)

except KeyboardInterrupt:
    print("Productor detenido.")

finally:
    productor.close()
