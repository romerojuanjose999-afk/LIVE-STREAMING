
#PRODUCTOR 2 · Super Chats + espectadores reales (fuente distinta al productor 1)
#Topic de Kafka: chat-espectadores
#Junta dos orígenes de datos que NO vienen del chat simulado:
# Super Chats: donaciones con monto en COP (generadas cada pocos segundos).
# Espectadores reales: lo que escriben las personas conectadas a la URL pública de la web (cada una con su propio nombre).

import random
import re
from collections import defaultdict

from config import (GROSERIAS_GENERADAS, MENSAJES_SPAM, MENSAJES_SUPERCHAT, MONTOS_SUPERCHAT, PLANTILLAS_TOXICAS,
                    PROB_SUPERCHAT_TOXICO, TOPIC_ESPECTADORES, Color as C)
from kafka_utils import ProductorKafka, ahora_iso


class ProductorEspectadores(ProductorKafka):

    PROB_SUPERCHAT = 0.12   # probabilidad de un Super Chat en cada vuelta de 0,5 s (≈ uno cada 4 s)

    def __init__(self):
        super().__init__(nombre="productor_espectadores", topic=TOPIC_ESPECTADORES, color=C.Y)
        self.donantes = []
        self.nombres_por_cliente = defaultdict(set)   # cada celular/navegador y los nombres que ha usado
        print(f"{C.D}Fuentes: Super Chats simulados + mensajes de espectadores desde la web{C.X}")

    @staticmethod
    def limpiar_nombre(nombre):
        """Nombre seguro para el chat: letras, números y _ (máx. 20)."""
        nombre = re.sub(r"[^\w]", "_", (nombre or "").strip())[:20].strip("_")
        return nombre or "Espectador"

    def superchat(self):
        if len(self.donantes) < 6 or random.random() < .3:
            self.donantes.append(self.nombre_aleatorio(self.donantes))
        candidatos = [d for d in self.donantes if d not in self.baneados] or [self.nombre_aleatorio(self.donantes)]
        usuario = random.choice(candidatos)
        if random.random() * 100 < PROB_SUPERCHAT_TOXICO:
            texto = random.choice([random.choice(PLANTILLAS_TOXICAS).format(w=random.choice(GROSERIAS_GENERADAS)),
                                   random.choice(MENSAJES_SPAM)])
        else:
            texto = random.choice(MENSAJES_SUPERCHAT)
        return {"usuario": usuario, "mensaje": texto, "tipo": "superchat", "monto": random.choice(MONTOS_SUPERCHAT)}

    def generar_mensajes(self, comandos):
        mensajes = []
        # a) espectadores reales (vienen de la web, uno por cada persona conectada)
        for cmd in comandos:
            if cmd.get("tipo") != "espectador":
                continue
            usuario = self.limpiar_nombre(cmd.get("usuario"))
            cliente = str(cmd.get("cliente", ""))[:40]
            nombres = self.nombres_por_cliente[cliente] if cliente else {usuario}
            nombres.add(usuario)
            # expulsado = este nombre o cualquier otro nombre que haya usado el mismo celular
            if nombres & self.baneados:
                print(f"{C.R}[{ahora_iso()[11:]}] ✖ @{usuario} está expulsado (mismo dispositivo que "
                      f"@{sorted(nombres & self.baneados)[0]}) → mensaje bloqueado{C.X}")
                continue
            mensajes.append({"usuario": usuario, "mensaje": str(cmd.get("mensaje", ""))[:160], "tipo": "espectador"})

        # b) Super Chats simulados
        if not self.control["pausado"] and random.random() < self.PROB_SUPERCHAT:
            mensajes.append(self.superchat())
        return mensajes


if __name__ == "__main__":
    ProductorEspectadores().ejecutar()
