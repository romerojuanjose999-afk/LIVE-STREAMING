
#Deja todo en cero para empezar una demo limpia:
# borra los checkpoints de Spark (y la carpeta data/ de versiones anteriores, si existe)
# borra y vuelve a crear los topics de Kafka
# vacía las tablas de MySQL

import shutil
import time

from kafka.errors import UnknownTopicOrPartitionError

from config import CHECKPOINTS, DATA_VIEJA, TOPICS, Color as C
from database import ConexionMySQL, ConfiguracionBD
from kafka_utils import crear_topics, esperar_kafka

if input(f"{C.Y}¿Borrar checkpoints, topics de Kafka y tablas de MySQL? (s/n): {C.X}").strip().lower() != "s":
    raise SystemExit("Cancelado.")

shutil.rmtree(CHECKPOINTS, ignore_errors=True)
shutil.rmtree(DATA_VIEJA, ignore_errors=True)
print(f"{C.G}✔ Checkpoints de Spark borrados ({CHECKPOINTS}){C.X}")

admin = esperar_kafka()
try:
    existentes = [t for t in TOPICS if t in admin.list_topics()]
    if existentes:
        admin.delete_topics(existentes)
    for _ in range(15):   # Kafka borra los topics de forma asíncrona: esperar a que desaparezcan
        if not set(TOPICS) & set(admin.list_topics()):
            break
        time.sleep(1)
except UnknownTopicOrPartitionError:
    pass
admin.close()
crear_topics()
print(f"{C.G}✔ Topics de Kafka recreados: {', '.join(TOPICS)}{C.X}")

with ConexionMySQL(ConfiguracionBD.desde_entorno()) as bd:
    bd.crear_tablas()
    bd.vaciar_tablas()
print(f"{C.G}✔ Tablas de MySQL vacías (mensajes, expulsiones, lotes y estado de la web){C.X}\n")
