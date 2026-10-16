import time
from datetime import datetime

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from config import (asegurar_winutils, BANEADOS, CHECKPOINTS, DESBANEOS, EVENTOS, LEET_A, LEET_DE, MAX_FILES_POR_LOTE, METRICAS,
                    PATRON_ENLACE, PATRON_GROSERIA, PATRON_LAMBON, PATRON_PROMO, ruta_spark, SALIDA, STREAM_DIR, STRIKES_SPAM, TRIGGER,
                    VENTANAS, Color as C, agregar_jsonl, escribir_json, leer_json, leer_jsonl_desde,
                    preparar_carpetas)

# 1. SESIÓN DE SPARK
preparar_carpetas()
if not asegurar_winutils():          
    raise SystemExit(1)
spark = (SparkSession.builder
         .appName("LiveMod-Moderador-Chat")
         .master("local[*]")                              # usa todos los núcleos del PC
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

#  2. FUENTE (readStream)
esquema = StructType([
    StructField("offset", LongType()),
    StructField("usuario", StringType()),
    StructField("mensaje", StringType()),
    StructField("ts", TimestampType()),
])

chat = (spark.readStream
        .schema(esquema)
        .option("maxFilesPerTrigger", MAX_FILES_POR_LOTE)
        .json(ruta_spark(STREAM_DIR)))

# 3. TRANSFORMACIONES (reglas, sin estado)
letras = F.regexp_replace("mensaje", r"[^\p{L}]", "")
mayus = F.regexp_replace("mensaje", r"[^\p{Lu}]", "")
n_emojis = F.length("mensaje") - F.length(F.regexp_replace("mensaje", r"[\p{So}\x{1F300}-\x{1FAFF}]", ""))

clasificado = (chat
    # normalizar: minúsculas, sin tildes, leetspeak (g0n0rr3a → gonorrea) y sin letras repetidas
    .withColumn("norm", F.regexp_replace(F.translate(F.lower("mensaje"), LEET_DE, LEET_A), r"(.)\1+", "$1"))
    .withColumn("groseria", F.regexp_extract("norm", PATRON_GROSERIA, 0))
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

# 4. SINK: foreachBatch (lógica por micro-lote)
baneados = leer_json(BANEADOS, {"usuarios": {}})["usuarios"]    # estado que sobrevive entre lotes
strikes, ultimo_msg = {}, {}
_, desbaneos_leidos = leer_jsonl_desde(DESBANEOS, 0)
M = leer_json(METRICAS, {})
totales = M.get("totales", {"lotes": 0, "mensajes": 0, "aprobados": 0, "eliminados": 0, "expulsados": 0, "descartados": 0})
historial = M.get("historial", [])


def ahora():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def banear(usuario, motivo):
    baneados[usuario] = {"motivo": motivo, "hora": datetime.now().strftime("%H:%M:%S")}
    escribir_json(BANEADOS, {"usuarios": baneados})


def atender_desbaneos():
    global desbaneos_leidos
    nuevos, desbaneos_leidos = leer_jsonl_desde(DESBANEOS, desbaneos_leidos)
    for d in nuevos:
        u = d.get("usuario")
        if u in baneados:
            del baneados[u]
            strikes.pop(u, None)
            print(f"{C.G}↺ @{u} desbaneado manualmente por el moderador{C.X}")
    if nuevos:
        escribir_json(BANEADOS, {"usuarios": baneados})


def procesar_lote(df, batch_id):
    t0 = time.time()
    atender_desbaneos()

    # reglas dentro del lote con funciones de ventana: flood y mensajes repetidos
    por_usuario = Window.partitionBy("usuario").orderBy("offset")
    por_texto = Window.partitionBy("usuario", "mensaje").orderBy("offset")
    df = (df.withColumn("n_usuario", F.row_number().over(por_usuario))
            .withColumn("n_repetido", F.row_number().over(por_texto))
            .withColumn("motivo_spam",
                F.when(F.col("motivo_spam") != "", F.col("motivo_spam"))
                 .when((F.col("groseria") == "") & (F.col("n_repetido") > 1), "mensaje repetido")
                 .when((F.col("groseria") == "") & (F.col("n_usuario") > 4), "flood (demasiados mensajes)")
                 .otherwise(""))
            .withColumn("categoria", F.when(F.col("groseria") != "", "GROSERIA")
                                      .when(F.col("motivo_spam") != "", "SPAM").otherwise("OK"))
            .cache())

    filas = df.orderBy("offset").select("offset", "usuario", "mensaje", "ts", "groseria", "motivo_spam", "lambon").collect()
    if not filas:
        df.unpersist()
        return

    # SINK 1: historial completo en Parquet, particionado por categoría
    (df.select("offset", "usuario", "mensaje", "ts", "categoria", "groseria", "motivo_spam",
               F.lit(batch_id).alias("lote"))
       .write.mode("append").partitionBy("categoria").parquet(ruta_spark(SALIDA / "historial")))
    df.unpersist()

    # SINK 2: decisiones del moderador (estado entre lotes: baneados y strikes)
    eventos, cnt = [], {"aprobados": 0, "eliminados": 0, "expulsados": 0, "descartados": 0}
    lambones = 0
    for r in filas:
        u, texto = r["usuario"], r["mensaje"]
        ev = {"tipo": "mensaje", "lote": batch_id, "offset": r["offset"], "usuario": u, "mensaje": texto,
              "lag_ms": int((datetime.now() - r["ts"]).total_seconds() * 1000) if r["ts"] else None}
        motivo_spam = r["motivo_spam"] or ("mensaje repetido" if ultimo_msg.get(u) == texto else "")
        if u in baneados:
            ev.update(accion="DESCARTADO", motivo="usuario ya expulsado"); cnt["descartados"] += 1
        elif r["groseria"]:
            banear(u, f'grosería: "{r["groseria"]}"')
            ev.update(accion="EXPULSADO", motivo=f'grosería "{r["groseria"]}"'); cnt["eliminados"] += 1; cnt["expulsados"] += 1
        elif motivo_spam:
            strikes[u] = strikes.get(u, 0) + 1
            cnt["eliminados"] += 1
            if strikes[u] >= STRIKES_SPAM:
                banear(u, f"spam reiterado: {motivo_spam}")
                ev.update(accion="EXPULSADO", motivo=f"spam ({motivo_spam}) · strike {strikes[u]}/{STRIKES_SPAM}"); cnt["expulsados"] += 1
            else:
                ev.update(accion="ADVERTENCIA", motivo=f"spam ({motivo_spam}) · strike {strikes[u]}/{STRIKES_SPAM}")
        elif r["lambon"]:
            ev.update(accion="APROBADO", motivo="🍎 lambonería al profe Esteban", lambon=True); cnt["aprobados"] += 1; lambones += 1
        else:
            ev.update(accion="APROBADO", motivo=""); cnt["aprobados"] += 1
        ultimo_msg[u] = texto
        eventos.append(ev)

    dur = int((time.time() - t0) * 1000)
    lote = {"tipo": "lote", "lote": batch_id, "filas": len(filas), "duracion_ms": dur, "hora": ahora(),
            "desde": filas[0]["offset"], "hasta": filas[-1]["offset"], "lambones": lambones, **cnt}
    agregar_jsonl(EVENTOS, [lote] + eventos)

    totales["lotes"] += 1
    totales["mensajes"] += len(filas)
    for k in cnt:
        totales[k] += cnt[k]
    totales["lambones"] = totales.get("lambones", 0) + lambones
    historial.append(lote)
    del historial[:-30]
    escribir_json(METRICAS, {"totales": totales, "historial": historial, "ultimo_offset": filas[-1]["offset"],
                             "actualizado": ahora()})
    imprimir_lote(lote, eventos)


def imprimir_lote(lote, eventos):
    print(f"\n{C.LOTE} MICRO-LOTE #{lote['lote']} {C.X} {C.C}{lote['filas']} mensajes "
          f"(offset {lote['desde']}→{lote['hasta']}) · procesado en {lote['duracion_ms']} ms{C.X}")
    for e in eventos:
        cab = f"{C.M}offset:{e['offset']:05d}{C.X} {C.BOLD}@{e['usuario']}{C.X}"
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


#5. QUERY 2: ventanas de tiempo (con estado)
ventanas = {}


def guardar_ventanas(df, batch_id):
    for r in df.collect():
        k = r["inicio"]
        ventanas.setdefault(k, {"inicio": k, "fin": r["fin"], "OK": 0, "SPAM": 0, "GROSERIA": 0})[r["categoria"]] = r["count"]
    for k in sorted(ventanas)[:-12]:
        del ventanas[k]
    escribir_json(VENTANAS, [ventanas[k] for k in sorted(ventanas)])


conteo_ventanas = (clasificado
    .withWatermark("ts", "30 seconds")
    .groupBy(F.window("ts", "10 seconds"), "categoria")
    .count()
    .select(F.date_format("window.start", "HH:mm:ss").alias("inicio"),
            F.date_format("window.end", "HH:mm:ss").alias("fin"), "categoria", "count"))

# 6. ARRANCAR LAS DOS QUERIES
print(f"{C.G}{C.BOLD}$ python moderador_spark.py   (Apache Spark {spark.version}){C.X}")
print(f"{C.D}Fuente: {STREAM_DIR}  ·  trigger: {TRIGGER}  ·  maxFilesPerTrigger: {MAX_FILES_POR_LOTE}{C.X}")
print(f"{C.D}Política: grosería = expulsión inmediata · spam = {STRIKES_SPAM} strikes · checkpoints en data/checkpoints{C.X}")
print(f"{C.D}Spark UI: http://localhost:4040  ·  esperando mensajes...{C.X}")

q1 = (clasificado.writeStream
      .queryName("moderacion")
      .foreachBatch(procesar_lote)
      .option("checkpointLocation", ruta_spark(CHECKPOINTS / "moderacion"))
      .trigger(processingTime=TRIGGER)
      .start())

q2 = (conteo_ventanas.writeStream
      .queryName("ventanas")
      .outputMode("update")
      .foreachBatch(guardar_ventanas)
      .option("checkpointLocation", ruta_spark(CHECKPOINTS / "ventanas"))
      .trigger(processingTime="2 seconds")
      .start())

try:
    spark.streams.awaitAnyTermination()
except (KeyboardInterrupt, Exception):          # Ctrl + C
    print(f"\n{C.Y}Spark detenido. El checkpoint guardó por dónde iba: al relanzarlo retoma desde ahí.{C.X}")
    try:
        q1.stop(); q2.stop(); spark.stop()
    except Exception:
        pass
