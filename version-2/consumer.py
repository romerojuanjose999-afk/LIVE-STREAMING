"""
CONSUMIDOR ÚNICO · Moderador del chat con Spark Structured Streaming

    Kafka (chat-bots + chat-espectadores)  →  Spark  →  MySQL (por lotes, JDBC)
                                                    →  web (métricas en vivo)

1. readStream de Kafka suscrito a los 2 topics (los 2 productores).
2. Transformaciones con Spark SQL: normaliza el texto y detecta groserías / spam.
3. foreachBatch: en cada micro-lote decide (aprobar, advertir, expulsar) y
   guarda el lote completo en MySQL con DataFrame.write.jdbc.
4. Query 2: conteos por ventanas de 10 s (window + watermark).

    python consumer.py
"""
import time
import traceback
from datetime import datetime

import pyspark
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from config import (CHECKPOINTS, FLOOD_MAX, FLOOD_SEGUNDOS, KAFKA_SERVIDOR, LEET_A, LEET_DE, MAX_OFFSETS_POR_LOTE,
                    PATRON_ENLACE, PATRON_GROSERIA, PATRON_GROSERIA_COMPACTA, PATRON_GROSERIA_DOBLE, PATRON_LAMBON,
                    PATRON_PROMO, REPETIDO_SEGUNDOS, STRIKES_SPAM, TOPIC_CHAT, TOPICS, TRIGGER, Color as C,
                    asegurar_winutils, preparar_carpetas, ruta_spark)
from database import ConexionMySQL, ConfiguracionBD
from database.estado import EstadoCompartido
from database.repositorio import RepositorioModeracion

VERSION_MYSQL_JDBC = "8.4.0"
CORTO = {TOPIC_CHAT: "bots"}   # nombre corto de cada topic para la consola


def corto(topic):
    return CORTO.get(topic, "esp")


# ══════════════════════════════════════════════════════════════════ 1. SESIÓN DE SPARK
def crear_sesion_spark():
    scala = "2.13" if int(pyspark.__version__.split(".")[0]) >= 4 else "2.12"
    paquetes = [f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{pyspark.__version__}",   # conector Kafka
                f"com.mysql:mysql-connector-j:{VERSION_MYSQL_JDBC}"]                       # driver JDBC MySQL
    spark = (SparkSession.builder
             .appName("LiveMod-Consumer")
             .master("local[*]")                              # usa todos los núcleos del PC
             .config("spark.jars.packages", ",".join(paquetes))
             .config("spark.sql.shuffle.partitions", "1")     # datos pequeños: 1 partición = lotes más rápidos
             .config("spark.driver.host", "127.0.0.1")        # Spark habla consigo mismo por localhost,
             .config("spark.driver.bindAddress", "127.0.0.1") # aunque cambie la red Wi-Fi
             .config("spark.python.worker.reuse", "true")     # reutiliza el proceso Python entre lotes
             .config("spark.sql.session.timeZone", "America/Bogota")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ══════════════════════════════════════════════════════════════════ 2. FUENTE: KAFKA
ESQUEMA_MENSAJE = StructType([
    StructField("usuario", StringType()),
    StructField("mensaje", StringType()),
    StructField("tipo", StringType()),        # chat | superchat | espectador
    StructField("monto", IntegerType()),      # solo Super Chats
])


def leer_kafka(spark):
    crudo = (spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_SERVIDOR)
             .option("subscribe", ",".join(TOPICS))            # ← los 2 productores
             .option("startingOffsets", "earliest")            # 1.ª vez: desde el inicio; luego manda el checkpoint
             .option("maxOffsetsPerTrigger", MAX_OFFSETS_POR_LOTE)
             .option("failOnDataLoss", "false")
             .load())
    return parsear_kafka(crudo)


def parsear_kafka(crudo):
    """Kafka entrega bytes: se convierte el "value" (JSON) a columnas."""
    return (crudo
            .select(F.col("topic").alias("fuente"), "offset", F.col("timestamp").alias("ts"),
                    F.from_json(F.col("value").cast("string"), ESQUEMA_MENSAJE).alias("d"))
            .select("fuente", "offset", "ts", "d.usuario", "d.mensaje",
                    F.coalesce("d.tipo", F.lit("chat")).alias("tipo"), "d.monto")
            .where(F.col("usuario").isNotNull() & F.col("mensaje").isNotNull()))


# ══════════════════════════════════════════════════════════════════ 3. TRANSFORMACIONES (sin estado)
def clasificar(chat):
    letras = F.regexp_replace("mensaje", r"[^\p{L}]", "")
    mayus = F.regexp_replace("mensaje", r"[^\p{Lu}]", "")
    n_emojis = F.length("mensaje") - F.length(F.regexp_replace("mensaje", r"[\p{So}\x{1F300}-\x{1FAFF}]", ""))
    return (chat
        # base: minúsculas, sin tildes y leetspeak (g0n0rr3a → gonorrea)
        .withColumn("base", F.translate(F.lower("mensaje"), LEET_DE, LEET_A))
        # norm: además sin letras repetidas (gonorreaaa → gonorea)
        .withColumn("norm", F.regexp_replace("base", r"(.)\1+", "$1"))
        # compacto: solo letras, para pillar "G o n o r r e a" o "g.o.n.o.r.r.e.a"
        .withColumn("compacto", F.regexp_replace(F.regexp_replace("base", "[^a-zñ]", ""), r"(.)\1+", "$1"))
        # grosería = la primera de las 3 búsquedas que encuentre algo
        .withColumn("groseria", F.coalesce(
            F.nullif(F.regexp_extract("norm", PATRON_GROSERIA, 0), F.lit("")),
            F.nullif(F.regexp_extract("base", PATRON_GROSERIA_DOBLE, 0), F.lit("")),
            F.nullif(F.regexp_extract("compacto", PATRON_GROSERIA_COMPACTA, 0), F.lit("")),
            F.lit("")))
        .withColumn("lambon", F.col("norm").rlike(PATRON_LAMBON))
        .withColumn("motivo_spam",
            F.when(F.col("mensaje").rlike(PATRON_ENLACE), "enlace externo")
             .when(F.col("norm").rlike(PATRON_PROMO), "autopromoción")
             .when(n_emojis >= 6, "flood de emojis")
             .when(F.col("mensaje").rlike(r"(.)\1{7,}"), "caracteres repetidos")
             .when((F.length(letras) >= 12) & (F.length(mayus) / F.length(letras) > 0.75), "mayúsculas excesivas")
             .otherwise(""))
        .withColumn("categoria",
            F.when(F.col("groseria") != "", "GROSERIA")
             .when(F.col("motivo_spam") != "", "SPAM")
             .otherwise("OK")))


# ══════════════════════════════════════════════════════════════════ 4. SINK: foreachBatch (con estado)
class ModeradorChat:
    """Toma las decisiones de cada micro-lote y las guarda en MySQL."""

    def __init__(self, repositorio, estado):
        self.repo = repositorio
        self.estado = estado                       # EstadoCompartido: baneados, eventos y métricas en MySQL
        self.baneados = self.estado.baneados()     # estado que sobrevive entre lotes (y entre reinicios)
        self.strikes, self.ultimo_msg = {}, {}
        m = self.estado.leer_valor("metricas", {})
        self.totales = m.get("totales", {"lotes": 0, "mensajes": 0, "aprobados": 0, "eliminados": 0,
                                         "expulsados": 0, "descartados": 0})
        self.historial = m.get("historial", [])

    # -------------------------------------------------------------- estado de expulsiones
    def banear(self, usuario, motivo):
        self.baneados[usuario] = {"motivo": motivo, "hora": datetime.now().strftime("%H:%M:%S")}
        self.estado.banear(usuario, motivo)        # tabla baneados: la leen los productores y la web

    def atender_desbaneos(self):
        """La web desbanea borrando la fila en MySQL; aquí se recarga la tabla al inicio de cada lote."""
        actuales = self.estado.baneados()
        for u in set(self.baneados) - set(actuales):
            self.strikes.pop(u, None)
            print(f"{C.G}↺ @{u} desbaneado manualmente por el moderador{C.X}")
        self.baneados = actuales

    # -------------------------------------------------------------- lógica de cada lote
    def decidir(self, r):
        """Regla de negocio para UN mensaje. Devuelve (accion, motivo)."""
        u, texto = r["usuario"], r["mensaje"]
        motivo_spam = r["motivo_spam"] or ("mensaje repetido" if self.ultimo_msg.get(u) == texto else "")
        self.ultimo_msg[u] = texto
        if u in self.baneados:
            return "DESCARTADO", "usuario ya expulsado"
        if r["groseria"]:
            self.banear(u, f'grosería: "{r["groseria"]}"')
            return "EXPULSADO", f'grosería "{r["groseria"]}"'
        if motivo_spam:
            self.strikes[u] = self.strikes.get(u, 0) + 1
            if self.strikes[u] >= STRIKES_SPAM:
                self.banear(u, f"spam reiterado: {motivo_spam}")
                return "EXPULSADO", f"spam ({motivo_spam}) · strike {self.strikes[u]}/{STRIKES_SPAM}"
            return "ADVERTENCIA", f"spam ({motivo_spam}) · strike {self.strikes[u]}/{STRIKES_SPAM}"
        if r["lambon"]:
            return "APROBADO", "🍎 lambonería al profe Esteban"
        return "APROBADO", ""

    def procesar_lote(self, df, batch_id):
        """Envoltorio de seguridad: si un lote falla se muestra el error y el streaming SIGUE vivo."""
        try:
            self._procesar_lote(df, batch_id)
        except Exception as e:
            print(f"\n{C.ALERTA} ERROR en el micro-lote #{batch_id} {C.X} {C.R}{type(e).__name__}: {str(e).splitlines()[0][:200]}{C.X}")
            print(f"{C.D}{traceback.format_exc(limit=4)}{C.X}")
            try:
                self.estado.agregar_eventos([{"tipo": "mysql", "lote": batch_id, "filas": 0, "ms": 0, "ok": False}])
            except Exception:
                pass

    def _procesar_lote(self, df, batch_id):
        t0 = time.time()
        self.atender_desbaneos()

        # Reglas con funciones de ventana POR TIEMPO (no dependen del tamaño del lote):
        #   flood    = más de FLOOD_MAX mensajes del mismo usuario en FLOOD_SEGUNDOS
        #   repetido = el mismo texto del mismo usuario dentro de REPETIDO_SEGUNDOS
        segundos = F.col("ts").cast("long")
        ventana_flood = Window.partitionBy("usuario").orderBy(segundos).rangeBetween(-FLOOD_SEGUNDOS, 0)
        ventana_repetido = Window.partitionBy("usuario", "mensaje").orderBy(segundos).rangeBetween(-REPETIDO_SEGUNDOS, 0)
        df = (df.withColumn("msgs_recientes", F.count("*").over(ventana_flood))
                .withColumn("repeticiones", F.count("*").over(ventana_repetido))
                .withColumn("motivo_spam",
                    F.when(F.col("motivo_spam") != "", F.col("motivo_spam"))
                     .when((F.col("groseria") == "") & (F.col("repeticiones") > 1), "mensaje repetido")
                     .when((F.col("groseria") == "") & (F.col("msgs_recientes") > FLOOD_MAX), "flood (demasiados mensajes)")
                     .otherwise(""))
                .withColumn("categoria", F.when(F.col("groseria") != "", "GROSERIA")
                                          .when(F.col("motivo_spam") != "", "SPAM").otherwise("OK")))

        filas = df.orderBy("ts", "offset").select("fuente", "offset", "ts", "usuario", "mensaje", "tipo", "monto",
                                                  "groseria", "motivo_spam", "lambon", "categoria").collect()
        if not filas:
            return

        ahora = datetime.now()
        eventos, filas_bd, expulsiones = [], [], []
        cnt = {"aprobados": 0, "eliminados": 0, "expulsados": 0, "descartados": 0}
        lambones = superchats = monto_total = 0

        for r in filas:
            accion, motivo = self.decidir(r)
            lag = int((ahora - r["ts"]).total_seconds() * 1000) if r["ts"] else None
            if accion == "APROBADO":
                cnt["aprobados"] += 1
            elif accion == "DESCARTADO":
                cnt["descartados"] += 1
            else:
                cnt["eliminados"] += 1
            if accion == "EXPULSADO":
                cnt["expulsados"] += 1
                expulsiones.append((batch_id, r["usuario"], r["fuente"], motivo[:200], ahora))
            es_lambon = accion == "APROBADO" and r["lambon"]
            lambones += es_lambon
            if r["tipo"] == "superchat":
                superchats += 1
                monto_total += r["monto"] or 0

            eventos.append({"tipo": "mensaje", "lote": batch_id, "fuente": r["fuente"], "offset": r["offset"],
                            "usuario": r["usuario"], "mensaje": r["mensaje"], "origen": r["tipo"],
                            "monto": r["monto"], "lag_ms": lag, "accion": accion, "motivo": motivo,
                            **({"lambon": True} if es_lambon else {})})
            filas_bd.append((batch_id, r["fuente"], r["offset"], r["usuario"], r["mensaje"][:500], r["tipo"],
                             r["monto"], r["categoria"], accion, motivo[:200], lag, r["ts"], ahora))

        dur = int((time.time() - t0) * 1000)
        rangos = {}
        for r in filas:
            a, b = rangos.get(r["fuente"], (r["offset"], r["offset"]))
            rangos[r["fuente"]] = (min(a, r["offset"]), max(b, r["offset"]))
        rango = " · ".join(f"{corto(t)} {a}→{b}" for t, (a, b) in sorted(rangos.items()))

        # 1) PRIMERO se publica la decisión para la web: el chat se actualiza sin esperar a MySQL
        lote = {"tipo": "lote", "lote": batch_id, "filas": len(filas), "duracion_ms": dur,
                "hora": ahora.strftime("%H:%M:%S.%f")[:-3], "rango": rango, "lambones": lambones,
                "superchats": superchats, "monto": monto_total, **cnt}
        self.estado.agregar_eventos([lote] + eventos)   # tabla eventos_web: la lee la página
        self.imprimir_lote(lote, eventos)

        # 2) DESPUÉS se guarda el lote completo en MySQL (Spark JDBC, INSERT por lotes)
        lote_bd = (batch_id, len(filas), cnt["aprobados"], cnt["eliminados"], cnt["expulsados"],
                   cnt["descartados"], superchats, monto_total, dur, ahora)
        t_bd = time.time()
        try:
            guardadas = self.repo.guardar_todo(filas_bd, expulsiones, lote_bd)
            bd_ok = True
        except Exception as e:      # si MySQL falla, el chat sigue funcionando y se avisa en consola
            guardadas, bd_ok = 0, False
            print(f"{C.ALERTA} MySQL {C.X} {C.R}no se pudo guardar el lote #{batch_id}: {str(e).splitlines()[0][:160]}{C.X}")
        ms_bd = int((time.time() - t_bd) * 1000)
        self.estado.agregar_eventos([{"tipo": "mysql", "lote": batch_id, "filas": guardadas, "ms": ms_bd, "ok": bd_ok}])
        if bd_ok:
            print(f"  {C.G}💾 MySQL ← INSERT por lotes: {guardadas} filas (mensajes + expulsiones + lotes) "
                  f"en {ms_bd} ms{C.X}")

        t = self.totales
        t["lotes"] += 1
        t["mensajes"] += len(filas)
        for k in cnt:
            t[k] += cnt[k]
        t["lambones"] = t.get("lambones", 0) + lambones
        t["superchats"] = t.get("superchats", 0) + superchats
        t["monto"] = t.get("monto", 0) + monto_total
        t["guardados_mysql"] = t.get("guardados_mysql", 0) + guardadas
        self.historial.append(lote)
        del self.historial[:-30]
        self.estado.guardar_valor("metricas", {"totales": t, "historial": self.historial, "actualizado": lote["hora"]})

    @staticmethod
    def imprimir_lote(lote, eventos):
        print(f"\n{C.LOTE} MICRO-LOTE #{lote['lote']} {C.X} {C.C}{lote['filas']} mensajes ({lote['rango']}) "
              f"· {lote['duracion_ms']} ms{C.X}")
        for e in eventos:
            cab = f"{C.M}{corto(e['fuente'])}:{e['offset']:05d}{C.X} {C.BOLD}@{e['usuario']}{C.X}"
            if e.get("monto"):
                cab += f" {C.Y}💲{e['monto']:,}{C.X}"
            if e.get("lambon"):
                print(f"  {C.Y}🍎 LAMBÓN     {C.X}{cab} {C.Y}\"{e['mensaje']}\" → aprobado (lambonería detectada){C.X}")
            elif e["accion"] == "APROBADO":
                print(f"  {C.G}✔ APROBADO   {C.X}{cab} {C.D}\"{e['mensaje']}\" (lag {e['lag_ms']} ms){C.X}")
            elif e["accion"] == "ADVERTENCIA":
                print(f"  {C.Y}⚠ SPAM       {C.X}{cab} {C.Y}{e['motivo']} → mensaje eliminado{C.X}")
            elif e["accion"] == "EXPULSADO":
                print(f"  {C.ALERTA} 🚨 ALERTA {C.X} {cab} {C.R}{e['motivo']} → mensaje eliminado + USUARIO EXPULSADO{C.X}")
                print(f"               {C.D}texto: \"{e['mensaje']}\"{C.X}")
            else:
                print(f"  {C.D}· descartado {cab} (ya estaba expulsado){C.X}")


# 5. QUERY 2: ventanas de tiempo
class VentanasDeTiempo:
    def __init__(self, estado):
        self.estado = estado       # conexión propia: esta query corre en otro hilo
        self.ventanas = {}

    def guardar(self, df, batch_id):
        for r in df.collect():
            k = r["inicio"]
            self.ventanas.setdefault(k, {"inicio": k, "fin": r["fin"], "OK": 0, "SPAM": 0, "GROSERIA": 0})[r["categoria"]] = r["count"]
        for k in sorted(self.ventanas)[:-12]:
            del self.ventanas[k]
        self.estado.guardar_valor("ventanas", [self.ventanas[k] for k in sorted(self.ventanas)])


def conteo_por_ventanas(clasificado):
    return (clasificado
            .withWatermark("ts", "30 seconds")
            .groupBy(F.window("ts", "10 seconds"), "categoria")
            .count()
            .select(F.date_format("window.start", "HH:mm:ss").alias("inicio"),
                    F.date_format("window.end", "HH:mm:ss").alias("fin"), "categoria", "count"))


# ══════════════════════════════════════════════════════════════════ 6. ARRANQUE
def main():
    preparar_carpetas()
    if not asegurar_winutils():
        raise SystemExit(1)

    from kafka_utils import crear_topics
    crear_topics()                                   # por si el consumidor arranca antes que los productores

    config_bd = ConfiguracionBD.desde_entorno()
    with ConexionMySQL(config_bd) as bd:             # crea las tablas si no existen
        tablas = bd.crear_tablas()

    spark = crear_sesion_spark()
    repo = RepositorioModeracion(spark, config_bd)
    moderador = ModeradorChat(repo, EstadoCompartido(config_bd))
    ventanas = VentanasDeTiempo(EstadoCompartido(config_bd))

    clasificado = clasificar(leer_kafka(spark))

    print(f"{C.G}{C.BOLD}$ python consumer.py   (Apache Spark {spark.version}){C.X}")
    print(f"{C.D}Fuente: Kafka {KAFKA_SERVIDOR} · topics: {', '.join(TOPICS)} · trigger: {TRIGGER}{C.X}")
    print(f"{C.D}Estado compartido (expulsados, eventos, métricas) también en MySQL: sin archivos .json{C.X}")
    print(f"{C.D}Destino: {config_bd} · tablas: {', '.join(tablas)} · INSERT por lotes de hasta {config_bd.tamano_lote} filas{C.X}")
    print(f"{C.D}Política: grosería = expulsión inmediata · spam = {STRIKES_SPAM} strikes · checkpoints en {CHECKPOINTS}{C.X}")
    print(f"{C.D}Spark UI: http://localhost:4040  ·  esperando mensajes...{C.X}")

    q1 = (clasificado.writeStream
          .queryName("moderacion")
          .foreachBatch(moderador.procesar_lote)
          .option("checkpointLocation", ruta_spark(CHECKPOINTS / "moderacion_kafka"))
          .trigger(processingTime=TRIGGER)
          .start())

    q2 = (conteo_por_ventanas(clasificado).writeStream
          .queryName("ventanas")
          .outputMode("update")
          .foreachBatch(ventanas.guardar)
          .option("checkpointLocation", ruta_spark(CHECKPOINTS / "ventanas_kafka"))
          .trigger(processingTime="5 seconds")
          .start())

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:                        # Ctrl + C
        print(f"\n{C.Y}Spark detenido. El checkpoint guardó los offsets de Kafka: al relanzarlo retoma desde ahí.{C.X}")
    except Exception as e:                           # una query murió: mostrar la causa real
        print(f"\n{C.ALERTA} SPARK SE DETUVO POR UN ERROR {C.X} {C.R}{type(e).__name__}: {str(e).splitlines()[0][:300]}{C.X}")
        print(f"{C.D}{traceback.format_exc(limit=3)}{C.X}")
        print(f"{C.Y}Corrige el error y vuelve a lanzar: python consumer.py (retoma desde el checkpoint).{C.X}")
    finally:                                         # SIEMPRE apagar Spark (si no, el Java queda vivo)
        for cerrar in (q1.stop, q2.stop, spark.stop):
            try:
                cerrar()
            except Exception:
                pass
        print(f"{C.D}Spark apagado correctamente.{C.X}")


if __name__ == "__main__":
    main()
