#Utilidades de Kafka compartidas por los dos productores.
#crear_topics():   crea los topics del proyecto si todavía no existen.
#ProductorKafka:   clase base. Cada productor hereda de ella y solo implementa
#generar_mensajes(); el envío a Kafka, el registro para la
#web y el ciclo principal ya vienen hechos aquí.

import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

from config import KAFKA_SERVIDOR, PREFIJOS, SUFIJOS, TOPICS, Color as C
from database import ConfiguracionBD
from database.estado import EstadoCompartido


def ahora_iso():
    return datetime.now().isoformat(timespec="milliseconds")


def esperar_kafka(intentos=30):
    #Docker puede tardar unos segundos en levantar Kafka: reintenta antes de rendirse.
    for i in range(intentos):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_SERVIDOR, client_id="livemod-admin")
            return admin
        except NoBrokersAvailable:
            print(f"{C.Y}Esperando a Kafka en {KAFKA_SERVIDOR}... ({i + 1}/{intentos}){C.X}", flush=True)
            time.sleep(2)
    raise SystemExit(f"{C.R}✖ Kafka no responde en {KAFKA_SERVIDOR}. ¿Ejecutaste  docker compose up -d ?{C.X}")


def crear_topics(particiones=1):
    admin = esperar_kafka()
    existentes = set(admin.list_topics())
    nuevos = [NewTopic(name=t, num_partitions=particiones, replication_factor=1) for t in TOPICS if t not in existentes]
    if nuevos:
        try:
            admin.create_topics(nuevos)
            print(f"{C.G}✔ Topics creados en Kafka: {', '.join(t.name for t in nuevos)}{C.X}")
        except TopicAlreadyExistsError:
            pass   # otro productor lo creó al mismo tiempo
    admin.close()


class ProductorKafka:
    #Clase base de un productor: publica mensajes JSON en un topic de Kafka.

    INTERVALO = 0.5   # segundos entre envíos

    def __init__(self, nombre, topic, color):
        self.nombre = nombre          # nombre del script (para la consola)
        self.topic = topic            # topic de Kafka donde publica
        self.color = color
        self.n_envio = 0
        self.ultimo_texto = {}
        self.baneados = set()
        self.estado = EstadoCompartido(ConfiguracionBD.desde_entorno())   # control, baneados, comandos en MySQL
        self.control = self.estado.leer_control()
        self.ultimo_comando = self.estado.ultimo_comando()   # ignora comandos viejos
        crear_topics()
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVIDOR,
            client_id=nombre,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",          # Kafka confirma cuando el mensaje quedó guardado
            linger_ms=20,        # junta mensajes en pequeños lotes antes de enviarlos
        )

    #implementar por cada hijo
    def generar_mensajes(self, comandos):
        #Devuelve una lista de diccionarios {usuario, mensaje, ...} para enviar en esta vuelta.
        raise NotImplementedError

    # utilidades comunes
    def leer_estado(self):
        #Lee de MySQL los controles de la web, los expulsados y los comandos nuevos.
        self.control = self.estado.leer_control()
        nuevos_baneados = set(self.estado.baneados())
        for u in sorted(nuevos_baneados - self.baneados):
            print(f"{C.R}{C.BOLD}[{ahora_iso()[11:]}] ✖ @{u} fue EXPULSADO por el moderador → ya no puede escribir{C.X}")
        self.baneados = nuevos_baneados
        comandos, self.ultimo_comando = self.estado.comandos_desde(self.ultimo_comando)
        return comandos

    @staticmethod
    def nombre_aleatorio(existentes):
        nuevo = random.choice(PREFIJOS) + random.choice(SUFIJOS)
        return nuevo + str(random.randint(10, 999)) if nuevo in existentes else nuevo

    def enviar(self, mensajes):
        #Publica en Kafka y espera la confirmación para conocer el offset real de cada mensaje.
        if not mensajes:
            return
        self.n_envio += 1
        futuros = []
        for m in mensajes:
            m["ts"] = ahora_iso()
            m["fuente"] = self.topic
            futuros.append((m, self.producer.send(self.topic, key=m["usuario"], value=m)))
        self.producer.flush()

        registro = []
        envio = f"{self.topic} · envío #{self.n_envio}"
        print(f"{C.D}── {envio} ({len(mensajes)} msg) → Kafka{C.X}")
        for m, fut in futuros:
            meta = fut.get(timeout=10)
            self.ultimo_texto[m["usuario"]] = m["mensaje"]
            registro.append({**m, "topic": meta.topic, "offset": meta.offset, "archivo": envio})
            extra = f" {C.Y}💲{m['monto']:,} COP{C.X}" if m.get("monto") else ""
            print(f"{C.D}[{m['ts'][11:]}]{C.X} {self.color}PRODUCE →{C.X} {C.M}{meta.topic}:{meta.offset:05d}{C.X} "
                  f"{C.BOLD}@{m['usuario']}{C.X}{extra} {C.D}\"{m['mensaje']}\"{C.X}")
        self.estado.registrar_publicados(registro)   # para que la web lo muestre

    def ejecutar(self):
        print(f"{C.G}{C.BOLD}$ python {self.nombre}.py  →  Kafka {KAFKA_SERVIDOR}  ·  topic: {self.topic}{C.X}\n")
        try:
            while True:
                comandos = self.leer_estado()
                self.enviar(self.generar_mensajes(comandos))
                time.sleep(self.INTERVALO)
        except KeyboardInterrupt:
            print(f"\n{C.Y}{self.nombre} detenido.{C.X}")
        finally:
            self.producer.close()
