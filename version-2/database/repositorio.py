
#Repositorio: guarda los resultados de Spark en MySQL POR LOTES (nunca en archivos).

#mensajes (la tabla grande, todas las filas del micro-lote): Spark SQL con
 # DataFrame.write.jdbc(mode="append"), que envía INSERTs masivos (batchsize).
#expulsiones y lotes (pocas filas): un solo executemany de mysql-connector
  #también en lote, para no lanzar un trabajo de Spark por 1 o 2 filas.
from database.conexion import ConexionMySQL
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField, StructType, TimestampType)


class RepositorioModeracion:

    ESQUEMA_MENSAJES = StructType([
        StructField("lote_id", LongType()), StructField("fuente", StringType()),
        StructField("kafka_offset", LongType()), StructField("usuario", StringType()),
        StructField("mensaje", StringType()), StructField("tipo", StringType()),
        StructField("monto", IntegerType()), StructField("categoria", StringType()),
        StructField("accion", StringType()), StructField("motivo", StringType()),
        StructField("lag_ms", IntegerType()), StructField("fecha_mensaje", TimestampType()),
        StructField("fecha_proceso", TimestampType()),
    ])
    ESQUEMA_EXPULSIONES = StructType([
        StructField("lote_id", LongType()), StructField("usuario", StringType()),
        StructField("fuente", StringType()), StructField("motivo", StringType()),
        StructField("fecha", TimestampType()),
    ])
    ESQUEMA_LOTES = StructType([
        StructField("lote_id", LongType()), StructField("filas", IntegerType()),
        StructField("aprobados", IntegerType()), StructField("eliminados", IntegerType()),
        StructField("expulsados", IntegerType()), StructField("descartados", IntegerType()),
        StructField("superchats", IntegerType()), StructField("monto_total", LongType()),
        StructField("duracion_ms", IntegerType()), StructField("fecha_proceso", TimestampType()),
    ])

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config                       # objeto ConfiguracionBD
        self.bd = ConexionMySQL(config).conectar()  # conexión para las tablas pequeñas

    def _insertar_spark(self, filas, esquema, tabla):
        #Convierte las filas en DataFrame y las inserta en MySQL en un solo lote JDBC (Spark SQL).
        if not filas:
            return 0
        # 1 sola partición = Spark abre 1 solo proceso Python para convertir las filas.
        # (En Windows abrir varios a la vez es lento y pueden fallar por tiempo de espera.)
        rdd = self.spark.sparkContext.parallelize(filas, 1)
        df = self.spark.createDataFrame(rdd, esquema)
        df.write.jdbc(url=self.config.url_jdbc, table=tabla, mode="append",
                      properties=self.config.propiedades_jdbc())
        return len(filas)

    def _insertar_sql(self, filas, esquema, tabla):
        """INSERT ... VALUES (...),(...) en una sola sentencia con executemany."""
        if not filas:
            return 0
        columnas = [c.name for c in esquema.fields]
        sql = f"INSERT INTO {tabla} ({', '.join(columnas)}) VALUES ({', '.join(['%s'] * len(columnas))})"
        self.bd.conexion.ping(reconnect=True, attempts=3, delay=1)   # por si MySQL cerró la conexión
        cursor = self.bd.conexion.cursor()
        cursor.executemany(sql, filas)
        self.bd.conexion.commit()
        cursor.close()
        return len(filas)

    def guardar_mensajes(self, filas):
        try:
            return self._insertar_spark(filas, self.ESQUEMA_MENSAJES, "mensajes")     # Spark SQL + JDBC
        except Exception as e:
            # Plan B: si Spark JDBC falla, el lote se guarda igual (por lotes) y no se pierde nada
            print(f"\033[93m  ⚠ Spark JDBC falló ({str(e).splitlines()[0][:90]}) → se guarda con executemany\033[0m")
            return self._insertar_sql(filas, self.ESQUEMA_MENSAJES, "mensajes")

    def guardar_expulsiones(self, filas):
        return self._insertar_sql(filas, self.ESQUEMA_EXPULSIONES, "expulsiones")

    def guardar_lote(self, fila):
        return self._insertar_sql([fila], self.ESQUEMA_LOTES, "lotes")

    def guardar_todo(self, mensajes, expulsiones, lote):
        """Guarda las 3 tablas de un micro-lote. Devuelve cuántas filas quedaron en MySQL."""
        return self.guardar_mensajes(mensajes) + self.guardar_expulsiones(expulsiones) + self.guardar_lote(lote)
