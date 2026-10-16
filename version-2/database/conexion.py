#Conexión a MySQL desde Python (mysql-connector-python).
import time

import mysql.connector
from mysql.connector import Error


class ConexionMySQL:

    # Modelo de datos: 3 tablas
    TABLAS = {
        "lotes": """
            CREATE TABLE IF NOT EXISTS lotes (
                id            BIGINT AUTO_INCREMENT PRIMARY KEY,
                lote_id       BIGINT       NOT NULL,          -- batch_id de Spark
                filas         INT          NOT NULL,
                aprobados     INT          NOT NULL,
                eliminados    INT          NOT NULL,
                expulsados    INT          NOT NULL,
                descartados   INT          NOT NULL,
                superchats    INT          NOT NULL,
                monto_total   BIGINT       NOT NULL,          -- COP recaudados en el lote
                duracion_ms   INT          NOT NULL,
                fecha_proceso DATETIME(3)  NOT NULL
            ) DEFAULT CHARSET = utf8mb4""",
        "mensajes": """
            CREATE TABLE IF NOT EXISTS mensajes (
                id            BIGINT AUTO_INCREMENT PRIMARY KEY,
                lote_id       BIGINT       NOT NULL,
                fuente        VARCHAR(40)  NOT NULL,          -- topic de Kafka (productor que lo envió)
                kafka_offset  BIGINT       NOT NULL,
                usuario       VARCHAR(60)  NOT NULL,
                mensaje       VARCHAR(500) NOT NULL,
                tipo          VARCHAR(20)  NOT NULL,          -- chat | superchat | espectador
                monto         INT          NULL,
                categoria     VARCHAR(20)  NOT NULL,          -- OK | SPAM | GROSERIA
                accion        VARCHAR(20)  NOT NULL,          -- APROBADO | ADVERTENCIA | EXPULSADO | DESCARTADO
                motivo        VARCHAR(200) NOT NULL,
                lag_ms        INT          NULL,
                fecha_mensaje DATETIME(3)  NULL,
                fecha_proceso DATETIME(3)  NOT NULL,
                INDEX idx_usuario (usuario),
                INDEX idx_accion (accion),
                INDEX idx_kafka (fuente, kafka_offset)
            ) DEFAULT CHARSET = utf8mb4""",
        "expulsiones": """
            CREATE TABLE IF NOT EXISTS expulsiones (
                id            BIGINT AUTO_INCREMENT PRIMARY KEY,
                lote_id       BIGINT       NOT NULL,
                usuario       VARCHAR(60)  NOT NULL,
                fuente        VARCHAR(40)  NOT NULL,
                motivo        VARCHAR(200) NOT NULL,
                fecha         DATETIME(3)  NOT NULL
            ) DEFAULT CHARSET = utf8mb4""",
        # estado compartido entre productores, consumidor y web 
        "baneados": """
            CREATE TABLE IF NOT EXISTS baneados (
                usuario       VARCHAR(60)  PRIMARY KEY,       -- usuarios expulsados AHORA (desbanear = DELETE)
                motivo        VARCHAR(200) NOT NULL,
                hora          VARCHAR(8)   NOT NULL
            ) DEFAULT CHARSET = utf8mb4""",
        "control": """
            CREATE TABLE IF NOT EXISTS control (
                id            TINYINT      PRIMARY KEY,       -- una sola fila (id = 1)
                tasa          INT          NOT NULL,          -- mensajes por segundo del productor 1
                toxicos       INT          NOT NULL,          -- % de mensajes tóxicos
                pausado       BOOLEAN      NOT NULL
            ) DEFAULT CHARSET = utf8mb4""",
        "comandos": """
            CREATE TABLE IF NOT EXISTS comandos (
                id            BIGINT AUTO_INCREMENT PRIMARY KEY,
                tipo          VARCHAR(20)  NOT NULL,          -- espectador | groseria | spam | lambon
                usuario       VARCHAR(60)  NULL,
                cliente       VARCHAR(40)  NULL,              -- identificador del celular/navegador
                mensaje       VARCHAR(500) NULL,                fecha         DATETIME(3)  NOT NULL
            ) DEFAULT CHARSET = utf8mb4""",
        "publicados": """
            CREATE TABLE IF NOT EXISTS publicados (
                id            BIGINT AUTO_INCREMENT PRIMARY KEY,
                topic         VARCHAR(40)  NOT NULL,          -- lo que cada productor envió a Kafka
                kafka_offset  BIGINT       NOT NULL,
                usuario       VARCHAR(60)  NOT NULL,
                mensaje       VARCHAR(500) NOT NULL,
                tipo          VARCHAR(20)  NOT NULL,
                monto         INT          NULL,
                envio         VARCHAR(60)  NOT NULL,
                ts            DATETIME(3)  NOT NULL
            ) DEFAULT CHARSET = utf8mb4""",
        "eventos_web": """
            CREATE TABLE IF NOT EXISTS eventos_web (
                id            BIGINT AUTO_INCREMENT PRIMARY KEY,
                lote_id       BIGINT       NOT NULL,
                tipo          VARCHAR(20)  NOT NULL,          -- lote | mensaje | mysql
                datos         TEXT         NOT NULL           -- detalle en JSON para la página web
            ) DEFAULT CHARSET = utf8mb4""",
        "estado_web": """
            CREATE TABLE IF NOT EXISTS estado_web (
                clave         VARCHAR(40)  PRIMARY KEY,       -- metricas | ventanas
                valor         MEDIUMTEXT   NOT NULL,
                actualizado   DATETIME(3)  NOT NULL
            ) DEFAULT CHARSET = utf8mb4""",
    }

    def __init__(self, config):
        self.config = config        # objeto ConfiguracionBD
        self.conexion = None

    def conectar(self, intentos=30):
        #Docker tarda unos segundos en levantar MySQL la primera vez: reintenta.
        for i in range(intentos):
            try:
                self.conexion = mysql.connector.connect(**self.config.parametros_python())
                self.conexion.autocommit = True   # cada programa ve al instante lo que escriben los otros
                return self
            except Error as e:
                print(f"Esperando a MySQL ({self.config})... {i + 1}/{intentos}  [{e.msg}]", flush=True)
                time.sleep(2)
        raise SystemExit(f"✖ No se pudo conectar a {self.config}. ¿Ejecutaste  docker compose up -d ?")

    def crear_tablas(self):
        cursor = self.conexion.cursor()
        for sql in self.TABLAS.values():
            cursor.execute(sql)
        self.conexion.commit()
        cursor.close()
        return list(self.TABLAS)

    def vaciar_tablas(self):
        cursor = self.conexion.cursor()
        for tabla in self.TABLAS:
            cursor.execute(f"TRUNCATE TABLE {tabla}")
        self.conexion.commit()
        cursor.close()

    def ejecutar(self, sql, parametros=None, muchos=False):
        #INSERT / UPDATE / DELETE. Con muchos=True ejecuta un lote (executemany).
        self.conexion.ping(reconnect=True, attempts=3, delay=1)
        cursor = self.conexion.cursor()
        if muchos:
            cursor.executemany(sql, parametros or [])
        else:
            cursor.execute(sql, parametros or ())
        filas = cursor.rowcount
        cursor.close()
        return filas

    def consultar(self, sql, parametros=None):
        self.conexion.ping(reconnect=True, attempts=3, delay=1)
        cursor = self.conexion.cursor(dictionary=True)
        cursor.execute(sql, parametros or ())
        filas = cursor.fetchall()
        cursor.close()
        return filas

    def cerrar(self):
        if self.conexion and self.conexion.is_connected():
            self.conexion.close()

    # permite usar:  with ConexionMySQL(config) as bd: ...
    def __enter__(self):
        return self.conectar()

    def __exit__(self, *args):
        self.cerrar()
