#Configuración de la base de datos (MySQL).
#ConfiguracionBD:            guarda los datos de conexión (constructor __init__)
#ConstructorConfiguracionBD: patrón Constructor (Builder) para armar la configuración paso a paso y de forma legible.



import os


class ConfiguracionBD:
    #Datos para conectarse a MySQL, tanto desde Python como desde Spark (JDBC).

    DRIVER_JDBC = "com.mysql.cj.jdbc.Driver"

    def __init__(self, host="localhost", puerto=3306, usuario="root", clave="",
                 base="livemod", tamano_lote=500):
        self.host = host
        self.puerto = int(puerto)
        self.usuario = usuario
        self.clave = clave
        self.base = base
        self.tamano_lote = int(tamano_lote)   # filas por INSERT en lote (batchsize de JDBC)

    @property
    def url_jdbc(self):
        #URL que usa Spark para escribir en MySQL.
        return (f"jdbc:mysql://{self.host}:{self.puerto}/{self.base}"
                "?useSSL=false&allowPublicKeyRetrieval=true"
                "&rewriteBatchedStatements=true"      # convierte N inserts en un solo INSERT ... VALUES (...),(...)
                "&characterEncoding=UTF-8&serverTimezone=America/Bogota")

    def propiedades_jdbc(self):
        return {"user": self.usuario, "password": self.clave, "driver": self.DRIVER_JDBC,
                "batchsize": str(self.tamano_lote)}

    def parametros_python(self):
        #Parámetros para mysql-connector-python (crear tablas y consultas).
        return {"host": self.host, "port": self.puerto, "user": self.usuario, "password": self.clave,
                "database": self.base, "charset": "utf8mb4", "collation": "utf8mb4_unicode_ci"}

    @classmethod
    def desde_entorno(cls):
        #Lee la configuración del archivo .env (o usa los valores por defecto del docker-compose).
        return (ConstructorConfiguracionBD()
                .servidor(os.getenv("MYSQL_HOST", "localhost"), os.getenv("MYSQL_PUERTO", "3306"))
                .credenciales(os.getenv("MYSQL_USUARIO", "root"), os.getenv("MYSQL_CLAVE", ""))
                .base_datos(os.getenv("MYSQL_BASE", "livemod"))
                .lote(os.getenv("MYSQL_TAMANO_LOTE", "500"))
                .construir())

    def __repr__(self):
        return f"MySQL {self.usuario}@{self.host}:{self.puerto}/{self.base}"   # sin mostrar la clave


class ConstructorConfiguracionBD:
    #Patrón Constructor: cada método fija una parte y devuelve self para encadenar.

    def __init__(self):
        self._datos = {}

    def servidor(self, host, puerto):
        self._datos.update(host=host, puerto=puerto)
        return self

    def credenciales(self, usuario, clave):
        self._datos.update(usuario=usuario, clave=clave)
        return self

    def base_datos(self, nombre):
        self._datos["base"] = nombre
        return self

    def lote(self, tamano):
        self._datos["tamano_lote"] = tamano
        return self

    def construir(self):
        return ConfiguracionBD(**self._datos)
