#PRODUCTOR  Comentarios del chat en vivo (fuente simulada)
#Topic de Kafka: chat-bots
#Simula a los usuarios del directo escribiendo: mensajes normales, groserías,
#spam y "lambonerías" al profe. La tasa y el % de tóxicos se controlan desde la web.


import random
from collections import defaultdict, deque

from config import (GROSERIAS_GENERADAS, MENSAJES_LAMBON, MENSAJES_NORMALES, MENSAJES_SPAM, MIN_ACTIVOS,
                    PLANTILLAS_TOXICAS, PROB_LAMBON, TOPIC_CHAT, USUARIOS, Color as C)
from kafka_utils import ProductorKafka, ahora_iso


class ProductorChat(ProductorKafka):

    def __init__(self):
        super().__init__(nombre="productor_chat", topic=TOPIC_CHAT, color=C.B)
        self.usuarios = list(USUARIOS)
        self.pausado_antes = None
        self.recientes = defaultdict(lambda: deque(maxlen=10))   # últimas frases de cada usuario
        self.ultimos_autores = deque(maxlen=8)                   # quién escribió hace poco
        print(f"{C.D}{len(self.usuarios)} usuarios en el chat · publicando cada {self.INTERVALO}s{C.X}")

    def generar_texto(self, tipo, usuario=None):
        if tipo == "groseria":
            return random.choice(PLANTILLAS_TOXICAS).format(w=random.choice(GROSERIAS_GENERADAS))
        if tipo == "spam":
            return random.choice(MENSAJES_SPAM)
        lista = MENSAJES_LAMBON if tipo == "lambon" or random.random() * 100 < PROB_LAMBON else MENSAJES_NORMALES
        # un usuario normal no repite sus últimas frases (si no, Spark lo marcaría como spam)
        opciones = [t for t in lista if t not in self.recientes[usuario]] or lista
        texto = random.choice(opciones)
        self.recientes[usuario].append(texto)
        return texto

    def reponer_usuarios(self, activos):
        """Si el moderador expulsó a muchos, entran espectadores nuevos al directo."""
        for _ in range(min(2, MIN_ACTIVOS - len(activos))):
            nuevo = self.nombre_aleatorio(self.usuarios)
            if nuevo not in self.usuarios:
                self.usuarios.append(nuevo)
                activos.append(nuevo)
                print(f"{C.G}[{ahora_iso()[11:]}] 👋 @{nuevo} entró al directo (activos: {len(activos)}){C.X}")

    def generar_mensajes(self, comandos):
        ctrl = self.control
        if ctrl["pausado"] != self.pausado_antes:
            print(f"{C.Y}{'⏸ Productor PAUSADO' if ctrl['pausado'] else '▶ Productor ACTIVO'} "
                  f"(tasa {ctrl['tasa']} msg/s · tóxicos {ctrl['toxicos']}%){C.X}")
            self.pausado_antes = ctrl["pausado"]

        activos = [u for u in self.usuarios if u not in self.baneados]
        if not ctrl["pausado"]:
            self.reponer_usuarios(activos)
        if not activos:
            return []

        mensajes = []
        # 1) mensajes forzados desde la web (botones "inyectar grosería / spam / lambón")
        for cmd in comandos:
            if cmd.get("tipo") in ("groseria", "spam", "lambon"):
                u = random.choice(activos)
                mensajes.append({"usuario": u, "mensaje": self.generar_texto(cmd["tipo"], u), "tipo": "chat"})

        # 2) mensajes automáticos según la tasa configurada
        if not ctrl["pausado"]:
            esperado = ctrl["tasa"] * self.INTERVALO
            n = int(esperado) + (1 if random.random() < esperado % 1 else 0)
            for _ in range(n):
                # como en un chat real: quien acaba de escribir espera un poco antes de volver a hacerlo
                u = random.choice([a for a in activos if a not in self.ultimos_autores] or activos)
                self.ultimos_autores.append(u)
                r = random.random() * 100
                tipo = "groseria" if r < ctrl["toxicos"] * .55 else "spam" if r < ctrl["toxicos"] else "normal"
                mensajes.append({"usuario": u, "mensaje": self.generar_texto(tipo, u),
                                 "tipo": "chat"})
        return mensajes


if __name__ == "__main__":
    ProductorChat().ejecutar()
