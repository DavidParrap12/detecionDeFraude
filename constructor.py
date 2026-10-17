from dataclasses import dataclass, asdict
from datetime import datetime
import uuid

LIMITE_POR_PERSONA = 100000


@dataclass
class Transaccion:
    id: str
    persona: str
    monto: int
    fecha: str

    @staticmethod
    def generar(persona, monto):
        return Transaccion(
            id=str(uuid.uuid4())[:8],
            persona=persona,
            monto=monto,
            fecha=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

    def como_diccionario(self):
        return asdict(self)


class DetectorFraude:
    def __init__(self, limite=LIMITE_POR_PERSONA):
        self.limite = limite
        self.acumulados = {}

    def analizar(self, transaccion):
        persona = transaccion["persona"]
        monto = int(transaccion["monto"])

        acumulado_anterior = self.acumulados.get(persona, 0)
        acumulado_actual = acumulado_anterior + monto

        self.acumulados[persona] = acumulado_actual

        sospechosa = acumulado_actual > self.limite

        return {
            **transaccion,
            "acumulado": acumulado_actual,
            "estado": "Sospechosa" if sospechosa else "Normal",
            "motivo": (
                f"El acumulado de {persona} supera "
                f"el límite de ${self.limite:,}"
                if sospechosa else ""
            )
        }
        