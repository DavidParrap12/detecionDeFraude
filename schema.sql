-- Esquema de base de datos para Detector de Fraudes

CREATE TABLE IF NOT EXISTS alertas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    persona TEXT NOT NULL,
    total INTEGER NOT NULL,
    riesgo TEXT NOT NULL,
    fecha TEXT NOT NULL,
    creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transacciones (
    id TEXT PRIMARY KEY,
    persona TEXT NOT NULL,
    monto INTEGER NOT NULL,
    fecha TEXT NOT NULL,
    creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para optimizar consultas frecuentes
CREATE INDEX IF NOT EXISTS idx_alertas_persona ON alertas(persona);
CREATE INDEX IF NOT EXISTS idx_alertas_riesgo ON alertas(riesgo);
CREATE INDEX IF NOT EXISTS idx_alertas_fecha ON alertas(fecha);
