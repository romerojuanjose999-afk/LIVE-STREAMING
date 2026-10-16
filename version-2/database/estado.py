
#Estado compartido entre los programas, guardado en MySQL 
# productores   publicados, (leen) control, baneados, comandos
#consumer.py  eventos_web, estado_web (métricas y ventanas), baneados
#servidor_web control, comandos, baneados (desbanear) y lee todo lo demás
#Cada programa crea su propio EstadoCompartido (su propia conexión). Un candado
#(Lock) permite usarlo desde varios hilos, como hace el servidor web.

import json
import threading
from datetime import datetime

from database.conexion import ConexionMySQL

CONTROL_DEFECTO = {"tasa": 4, "toxicos": 20, "pausado": False}


class EstadoCompartido:

    def __init__(self, config):
        self.bd = ConexionMySQL(config).conectar()
        self.bd.crear_tablas()
        self.candado = threading.Lock()

    # utilidades internas
    def _consultar(self, sql, parametros=None):
        with self.candado:
            return self.bd.consultar(sql, parametros)

    def _ejecutar(self, sql, parametros=None, muchos=False):
        with self.candado:
            return self.bd.ejecutar(sql, parametros, muchos)

    def _ultimo_id(self, tabla):
        return self._consultar(f"SELECT COALESCE(MAX(id), 0) AS n FROM {tabla}")[0]["n"]

    # control (web → productores)
    def leer_control(self):
        filas = self._consultar("SELECT tasa, toxicos, pausado FROM control WHERE id = 1")
        if not filas:
            return dict(CONTROL_DEFECTO)
        f = filas[0]
        return {"tasa": f["tasa"], "toxicos": f["toxicos"], "pausado": bool(f["pausado"])}

    def guardar_control(self, ctrl):
        self._ejecutar("REPLACE INTO control (id, tasa, toxicos, pausado) VALUES (1, %s, %s, %s)",
                       (int(ctrl["tasa"]), int(ctrl["toxicos"]), bool(ctrl["pausado"])))

    #baneados (consumer ↔ web ↔ productores)
    def baneados(self):
        return {f["usuario"]: {"motivo": f["motivo"], "hora": f["hora"]}
                for f in self._consultar("SELECT usuario, motivo, hora FROM baneados")}

    def banear(self, usuario, motivo):
        self._ejecutar("REPLACE INTO baneados (usuario, motivo, hora) VALUES (%s, %s, %s)",
                       (usuario, motivo[:200], datetime.now().strftime("%H:%M:%S")))

    def desbanear(self, usuario):
        self._ejecutar("DELETE FROM baneados WHERE usuario = %s", (usuario,))

    # comandos (web → productores)
    def agregar_comando(self, cmd):
        self._ejecutar("INSERT INTO comandos (tipo, usuario, cliente, mensaje, fecha) VALUES (%s, %s, %s, %s, %s)",
                       (str(cmd.get("tipo", ""))[:20], (cmd.get("usuario") or None),
                        (str(cmd.get("cliente"))[:40] if cmd.get("cliente") else None),
                        (str(cmd.get("mensaje"))[:500] if cmd.get("mensaje") is not None else None),
                        datetime.now()))

    def ultimo_comando(self):
        return self._ultimo_id("comandos")

    def comandos_desde(self, ultimo_id):
        #Comandos nuevos después de `ultimo_id`. Devuelve (lista, nuevo_ultimo_id).
        filas = self._consultar("SELECT id, tipo, usuario, cliente, mensaje FROM comandos WHERE id > %s ORDER BY id",
                                (ultimo_id,))
        return filas, (filas[-1]["id"] if filas else ultimo_id)

    # publicados (productores → web)
    def registrar_publicados(self, registros):
        self._ejecutar("INSERT INTO publicados (topic, kafka_offset, usuario, mensaje, tipo, monto, envio, ts) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                       [(r["topic"], r["offset"], r["usuario"], r["mensaje"][:500], r.get("tipo", "chat"),
                         r.get("monto"), r["archivo"], datetime.fromisoformat(r["ts"])) for r in registros],
                       muchos=True)

    def publicados_desde(self, desde, maximo=40):
        #Para la web: los nuevos después de `desde` (o los últimos `maximo` si desde < 0).
        columnas = "id, topic, kafka_offset AS `offset`, usuario, mensaje, tipo, monto, envio AS archivo, ts"
        if desde < 0:
            filas = self._consultar(f"SELECT * FROM (SELECT {columnas} FROM publicados ORDER BY id DESC LIMIT %s) t "
                                    "ORDER BY id", (maximo,))
        else:
            filas = self._consultar(f"SELECT {columnas} FROM publicados WHERE id > %s ORDER BY id LIMIT 500", (desde,))
        for f in filas:
            f["ts"] = f["ts"].isoformat(timespec="milliseconds")   # la web espera texto ISO
        return filas, self._ultimo_id("publicados")

    # eventos (consumer → web)
    def agregar_eventos(self, eventos):
        self._ejecutar("INSERT INTO eventos_web (lote_id, tipo, datos) VALUES (%s, %s, %s)",
                       [(e.get("lote", 0), e["tipo"], json.dumps(e, ensure_ascii=False, default=str)) for e in eventos],
                       muchos=True)

    def eventos_desde(self, desde, maximo=80):
        if desde < 0:
            filas = self._consultar("SELECT * FROM (SELECT id, datos FROM eventos_web ORDER BY id DESC LIMIT %s) t "
                                    "ORDER BY id", (maximo,))
        else:
            filas = self._consultar("SELECT id, datos FROM eventos_web WHERE id > %s ORDER BY id LIMIT 1000", (desde,))
        return [json.loads(f["datos"]) for f in filas], self._ultimo_id("eventos_web")

    # valores sueltos: métricas y ventanas
    def guardar_valor(self, clave, valor):
        self._ejecutar("REPLACE INTO estado_web (clave, valor, actualizado) VALUES (%s, %s, %s)",
                       (clave, json.dumps(valor, ensure_ascii=False, default=str), datetime.now()))

    def leer_valor(self, clave, defecto):
        filas = self._consultar("SELECT valor FROM estado_web WHERE clave = %s", (clave,))
        return json.loads(filas[0]["valor"]) if filas else defecto
