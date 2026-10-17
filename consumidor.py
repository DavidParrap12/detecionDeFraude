import json
import os
import sys

if "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

from datetime import datetime

import database

database.inicializar_db()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    sum as suma,
    window,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

# ==========================
# SPARK
# ==========================

spark = (
    SparkSession.builder
    .appName("DetectorFraudesKafka")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==========================
# ESQUEMA JSON
# ==========================

esquema = StructType([
    StructField("id", StringType(), True),
    StructField("persona", StringType(), True),
    StructField("monto", IntegerType(), True),
    StructField("fecha", StringType(), True)
])

# ==========================
# KAFKA
# ==========================

mensajes = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "transacciones"
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)

# ==========================
# TRANSACCIONES
# ==========================

transacciones = (
    mensajes
    .selectExpr(
        "CAST(value AS STRING) AS contenido"
    )
    .select(
        from_json(
            col("contenido"),
            esquema
        ).alias("dato")
    )
    .select("dato.*")
    .filter(
        col("persona").isNotNull()
    )
)

# ==========================
# ACUMULADOS POR PERSONA
# ==========================

acumulados = (
    transacciones
    .withColumn(
        "tiempo",
        to_timestamp(col("fecha"))
    )
    .withWatermark(
        "tiempo",
        "1 minute"
    )
    .groupBy(
        window(
            col("tiempo"),
            "10 minutes"
        ),
        col("persona")
    )
    .agg(
        suma("monto").alias("total")
    )
    .filter(
        col("total") > 100000
    )
)

# ==========================
# DETECCIÓN DE FRAUDE
# ==========================

def detectar_fraude(df, batch_id):

    filas = df.collect()

    if not filas:
        return

    alertas_nuevas = []

    for fila in filas:

        if fila["total"] > 300000:
            riesgo = "ALTO"

        elif fila["total"] > 200000:
            riesgo = "MEDIO"

        else:
            riesgo = "BAJO"

        alerta = {
            "persona": fila["persona"],
            "total": int(fila["total"]),
            "riesgo": riesgo,
            "fecha": datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        }

        alertas_nuevas.append(alerta)

        print(
            f"""
=========================
ALERTA DE FRAUDE
=========================
Persona: {fila['persona']}
Total acumulado: ${fila['total']}
Nivel de riesgo: {riesgo}
Fecha: {alerta['fecha']}
=========================
"""
        )

    archivo_alertas = "alertas.json"

    if os.path.exists(archivo_alertas):

        try:

            with open(
                archivo_alertas,
                "r",
                encoding="utf-8"
            ) as archivo:

                historial = json.load(archivo)

        except Exception:

            historial = []

    else:

        historial = []

    historial.extend(alertas_nuevas)

    with open(
        archivo_alertas,
        "w",
        encoding="utf-8"
    ) as archivo:

        json.dump(
            historial[-100:],
            archivo,
            ensure_ascii=False,
            indent=4
        )

    # Guardar en base de datos SQLite
    try:
        database.guardar_alertas_lote(alertas_nuevas)
    except Exception as e:
        print(f"Error al guardar alertas en base de datos: {e}")

# ==========================
# STREAMING
# ==========================

consulta = (
    acumulados.writeStream
    .foreachBatch(
        detectar_fraude
    )
    .outputMode("update")
    .start()
)

print(
    "Consumidor Spark conectado a Kafka."
)

consulta.awaitTermination()