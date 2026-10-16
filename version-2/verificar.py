
import os, re, shutil, socket, subprocess, sys

from config import BASE, KAFKA_SERVIDOR, PUERTO_WEB, asegurar_winutils

OK, MAL = "\033[92m✔\033[0m", "\033[91m✖\033[0m"
todo_bien = True


def check(nombre, cond, ayuda=""):
    global todo_bien
    print(f" {OK if cond else MAL} {nombre}")
    if not cond:
        todo_bien = False
        if ayuda:
            print(f"     → {ayuda}")


print("\n\033[1mVerificando LiveMod...\033[0m\n")

# 1. Python
v = sys.version_info
check(f"Python {v.major}.{v.minor}.{v.micro}", v >= (3, 9), "Instala Python 3.10 o superior desde python.org")

# 2. Java
java = shutil.which("java")
ver_java = 0
if java:
    out = subprocess.run([java, "-version"], capture_output=True, text=True).stderr
    m = re.search(r'version "(\d+)', out)
    ver_java = int(m.group(1)) if m else 0
check(f"Java {ver_java or 'no encontrado'}", ver_java >= 17,
      "Instala Java 17 o 21 (Adoptium Temurin) y reinicia VS Code para que tome el PATH")

# 3. PySpark
try:
    import pyspark
    check(f"PySpark {pyspark.__version__}", int(pyspark.__version__.split('.')[0]) >= 4 or ver_java < 21,
          "Con Java 21 usa PySpark 4:  pip install -r requirements.txt")
except ImportError:
    check("PySpark", False, "pip install -r requirements.txt")

# 4. Windows: winutils.exe + hadoop.dll
if os.name == "nt":
    check("winutils.exe y hadoop.dll (Hadoop para Windows)", asegurar_winutils(),
          "Revisa tu conexión a internet o cópialos a mano en la carpeta hadoop\\bin")
    check("Ruta del proyecto sin espacios ni OneDrive", " " not in str(BASE) and "onedrive" not in str(BASE).lower(),
          f"Recomendado mover la carpeta a C:\\livemod-spark (ahora está en {BASE})")

# 5. Librerías de Kafka y MySQL
for modulo, nombre in (("kafka", "kafka-python"), ("mysql.connector", "mysql-connector-python")):
    try:
        __import__(modulo)
        check(nombre, True)
    except ImportError:
        check(nombre, False, "pip install -r requirements.txt")


def puerto_abierto(host, puerto):
    s = socket.socket()
    s.settimeout(1)
    abierto = s.connect_ex((host, int(puerto))) == 0
    s.close()
    return abierto


# 6. Docker: Kafka y MySQL levantados
check("Docker instalado", shutil.which("docker") is not None, "Instala Docker Desktop y ábrelo")
host_k, puerto_k = KAFKA_SERVIDOR.split(":")
check(f"Kafka respondiendo en {KAFKA_SERVIDOR}", puerto_abierto(host_k, puerto_k), "docker compose up -d")
puerto_mysql = os.getenv("MYSQL_PUERTO", "3307")
check(f"MySQL respondiendo en localhost:{puerto_mysql}", puerto_abierto("127.0.0.1", puerto_mysql),
      "docker compose up -d   (la primera vez MySQL tarda ~30 s en iniciar)")

# 7. Puerto de la web libre
check(f"Puerto {PUERTO_WEB} libre para la web", not puerto_abierto("127.0.0.1", PUERTO_WEB),
      "Cierra lo que use ese puerto o cambia PUERTO_WEB en .env")

print()
if todo_bien:
    print("\033[92m\033[1mTodo listo.\033[0m Abre 4 terminales")
else:
    print("\033[93mCorrige lo marcado con ✖ y vuelve a ejecutar:  python verificar.py\033[0m\n")
