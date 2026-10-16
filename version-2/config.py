import os, re, sys, unicodedata
from pathlib import Path

BASE = Path(__file__).resolve().parent


if os.name == "nt":
    HADOOP_HOME = BASE / "hadoop"                      # incluidos en la carpeta hadoop/bin del proyecto
    os.environ["HADOOP_HOME"] = str(HADOOP_HOME)
    os.environ["PATH"] = str(HADOOP_HOME / "bin") + os.pathsep + os.environ.get("PATH", "")
    os.environ["PYSPARK_PYTHON"] = sys.executable      # evita el alias "python" de la Microsoft Store
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# consola en UTF-8 (emojis y tildes en PowerShell / Git Bash / terminal de VS Code)
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass


WINUTILS_URLS = [   # fuentes públicas de los binarios de Hadoop para Windows (se prueban en orden)
    "https://github.com/kontext-tech/winutils/raw/master/hadoop-3.4.0-win10-x64/bin/{f}",
    "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/{f}",
    "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/{f}",
]


def _binario_ok(p: Path) -> bool:
    try:
        return p.stat().st_size > 20_000 and p.read_bytes()[:2] == b"MZ"   # un .exe/.dll de Windows empieza por "MZ"
    except OSError:
        return False


def asegurar_winutils():
    """En Windows, Spark necesita hadoop/bin/winutils.exe y hadoop.dll. Si no están, los descarga una sola vez."""
    if os.name != "nt":
        return True
    import urllib.request
    carpeta = BASE / "hadoop" / "bin"
    carpeta.mkdir(parents=True, exist_ok=True)
    for f in ("winutils.exe", "hadoop.dll"):
        destino = carpeta / f
        if _binario_ok(destino):
            continue
        print(f"Descargando {f} (necesario para Spark en Windows)...", flush=True)
        for url in WINUTILS_URLS:
            try:
                urllib.request.urlretrieve(url.format(f=f), destino)
                if _binario_ok(destino):
                    print(f"  ✔ {f} listo ({url.split('/raw/')[0]})", flush=True)
                    break
            except Exception:
                pass
        if not _binario_ok(destino):
            print(f"\n✖ No se pudo descargar {f}. Descárgalo a mano desde\n"
                  f"  https://github.com/kontext-tech/winutils (carpeta hadoop-3.4.0-win10-x64/bin)\n"
                  f"  y cópialo en: {carpeta}\n")
            return False
    return True


def ruta_spark(p) -> str:
    """Spark/Hadoop prefieren rutas con / también en Windows (C:/Users/...)."""
    return str(p).replace("\\", "/")
# variables de entorno (.env)
def cargar_env(ruta=BASE / ".env"):
    """Lee el archivo .env (CLAVE=valor) sin librerías extra. Lo que ya esté en el sistema tiene prioridad."""
    if not ruta.exists():
        return
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


cargar_env()

#checkpoints de Spark
# Todos los datos van a MySQL. Lo único en disco es el checkpoint que Spark exige para
# recordar los offsets de Kafka; va FUERA del proyecto (carpeta del usuario) para no mezclarlo.
CHECKPOINTS = Path(os.getenv("LIVEMOD_CHECKPOINTS", Path.home() / ".livemod-spark" / "checkpoints"))
DATA_VIEJA = BASE / "data"   # carpeta de versiones anteriores (reiniciar.py la borra si existe)

#  Kafka
KAFKA_SERVIDOR = os.getenv("KAFKA_SERVIDOR", "localhost:9092")
TOPIC_CHAT = "chat-bots"                   # fuente 1: comentarios simulados del directo
TOPIC_ESPECTADORES = "chat-espectadores"   # fuente 2: Super Chats + espectadores reales de la web
TOPICS = [TOPIC_CHAT, TOPIC_ESPECTADORES]

#  Spark
TRIGGER = "1 second"          # cada cuánto Spark arma un micro-lote
MAX_OFFSETS_POR_LOTE = 50     # máximo de mensajes de Kafka por micro-lote (lotes pequeños = chat más fluido)
FLOOD_MAX = 8                 # flood: más de 8 mensajes del mismo usuario...
FLOOD_SEGUNDOS = 10           # en 10 segundos (por tiempo, no por tamaño del lote)
REPETIDO_SEGUNDOS = 20        # mismo texto del mismo usuario dentro de 20 s = spam
STRIKES_SPAM = 2              # spam: a la 2.ª vez se expulsa
PUERTO_WEB = int(os.getenv("PUERTO_WEB", "8000"))

CONTROL_DEFECTO = {"tasa": 4, "toxicos": 20, "pausado": False}

# datos del chat
USUARIOS = ["ElParcero_07", "Juanca_Tolima", "LaMonaGamer", "Pijao_Pro", "Ibague_Rider", "xX_Sniper_Xx",
            "CafeteroGG", "NicoFPS", "ValeStreams", "ElCalvo99", "Mafe_Plays", "DonPepe_TV", "Kevin_Motos",
            "LauraXD", "Bryan_Tuning", "SofiGamer", "ElPaisa23", "Dani_Speed", "Andres_Neon", "Tati_Live",
            "Zorro_Nocturno", "ManuHD", "Cami_Rush", "TioRico_404", "Santi_Drift", "NoobMaster_CO",
            "Pipe_Clutch", "Luisa_Kills"]
ESPECTADOR = "Tu_Espectador"
MIN_ACTIVOS = 18   # si quedan menos usuarios activos, entran espectadores nuevos al directo
PREFIJOS = ["Parce", "Gamer", "Neon", "Tolima", "Pijao", "Drift", "Turbo", "Mona", "Cafetero", "Rider", "Sniper",
            "Nitro", "Pixel", "Llanero", "Chamo", "Sombra", "Rayo", "Tigre", "Loco", "Crack"]
SUFIJOS = ["_CO", "_YT", "_TV", "99", "_07", "_Pro", "XD", "_23", "_GG", "_Live", "_HD", "_404"]

MENSAJES_NORMALES = [
    "que carro tan bacano 🔥", "jajaja casi se estrella", "saludos desde Ibagué 👋", "ese salto estuvo brutal",
    "dale a la misión del puerto", "cuántas horas lleva jugando?", "la música de la radio está buena",
    "vamos por la persecución 🚓", "qué gráficos tan buenos", "pilas con la policía atrás",
    "primera vez que veo el stream, buen contenido", "ese drift estuvo limpio", "compre el deportivo rojo",
    "saludos a todo el chat", "eso fue épico jajaja", "dale gas dale gas 🏎️", "buen stream parce",
    "a qué hora termina el directo?", "tremendo control del carro", "que no lo agarre la policía 😅",
    "ese edificio se ve increíble de noche", "like si te gusta el stream 👍", "me encanta este juego",
    "qué PC tiene para correrlo así?", "jajaja el NPC se asustó", "la próxima misión es la del banco?",
    "100% recomendado este canal", "buenas noches desde Medellín", "3 estrellas de búsqueda, corre!",
    "esa moto estaba volando", "ese tráiler está brutal 🔥", "Vice City se ve increíble 🌴",
    "ya quiero que salga el juego", "los gráficos de GTA VI son otro nivel", "esa playa se ve real",
    "volvimos a Vice City 🌴🌴"]
GROSERIAS_GENERADAS = ["gonorrea", "hijueputa", "malparido", "hp", "imbécil", "estúpido", "idiota", "mierda",
                       "pendejo", "carechimba", "huevón", "g0n0rr3a", "m4lp4r1d0", "hijueputaaa", "1d10ta"]
PLANTILLAS_TOXICAS = ["este streamer es un {w}", "{w} juegas horrible", "qué {w} tan malo para manejar",
                      "callate {w}", "ese man es un {w} jajaja", "{w} deja de chocar", "sos un {w} bro",
                      "quién ve a este {w}"]

MENSAJES_LAMBON = ["profe Esteban usted explica mejor que cualquier tutorial 🙌",
                   "saludos al profe Esteban, el mejor docente de la UNIMINUTO",
                   "profe Esteban, este proyecto merece un 5.0 ⭐", "el profe Esteban sí sabe de Spark, un crack",
                   "profe Esteban, gracias por enseñarnos streaming 🙏",
                   "si el profe Esteban juega esto seguro no lo agarra la policía",
                   "profe Esteban, con usted hasta Big Data se entiende fácil", "un aplauso para el profe Esteban 👏",
                   "profe Esteban, ¿nos regala la décima? jajaja",
                   "el profe Esteban procesa más rápido que Spark en micro-lotes",
                   "profe Esteban, su clase es la mejor del semestre", "Esteban para decano 🗳️",
                   "profe Esteban, qué buena la camisa de hoy", "el profe Esteban es el Spark de los docentes ⚡"]
PROB_LAMBON = 20
PATRON_LAMBON = r"(esteban|\bprofe\b)"

MENSAJES_SPAM = ["🔥🔥🔥 SIGUEME Y TE SIGO 🔥🔥🔥", "gana 500 mil diarios aquí 👉 www.plata-facil.xyz",
                 "COMPRA SEGUIDORES BARATOS bit.ly/segs-ya", "aaaaaaaaaaaaaaaaaaaaaaaa",
                 "JAJAJAJAJAJAJAJAJAJAJAJA", "suscríbanse a mi canal suscríbanse a mi canal",
                 "💰💰💰💰💰💰💰💰💰💰", "GANA DINERO DESDE CASA https://dinero-rapido.com"]

# Super Chats (productor 2): mensajes pagados, con monto en pesos colombianos
MONTOS_SUPERCHAT = [2000, 5000, 10000, 20000, 50000, 100000]
MENSAJES_SUPERCHAT = ["saludos desde Ibagué, sigue así 💪", "para la gasolina del deportivo 🏎️",
                      "primera vez que dono, excelente stream", "lee mi mensaje porfa 🙏",
                      "para que compres el carro de la misión", "feliz cumpleaños al streamer 🎉",
                      "aguante el Tolima ⚽", "saludos al profe Esteban desde el Super Chat 💸",
                      "para el café de la madrugada ☕", "eres el mejor en Vice City 🌴"]
PROB_SUPERCHAT_TOXICO = 15   # % de Super Chats que traen grosería o enlace (se moderan igual)

# reglas de moderación
# Groserías que se buscan DENTRO del texto (también dentro de palabras compuestas: triplehijueputa)
GROSERIAS = ["gonorrea", "hijueputa", "hijuepta", "jueputa", "malparido", "malparida", "malnacido", "malnacida",
             "imbecil", "estupido", "estupida", "idiota", "mierda", "pendejo", "pendeja", "carechimba", "careverga",
             "careculo", "culicagado", "mamaguevo", "mamahuevo", "huevon", "guevon", "marica", "marico", "maricon",
             "pirobo", "piroba", "gurrupleta", "lamecula"]
# Solo como palabra completa (cortas o que aparecen dentro de palabras normales: "computadora" tiene "puta")
GROSERIAS_CORTAS = ["hp", "hpta", "hpt", "jp", "ptm", "puta", "putas", "puto", "putos", "verga", "weon", "culo",
                    "bobohp", "sapohp"]
# Palabras donde la letra doble importa ("perra" ≠ "pera"): se buscan antes de quitar letras repetidas
GROSERIAS_DOBLES = ["perra", "perras", "zorra", "zorras"]

# Normalización (igual en Python y en Spark): minúsculas, sin tildes, leetspeak y letras repetidas
LEET_DE, LEET_A = "áéíóúü0431@5$7", "aeiouuoaeiasst"


def base_texto(texto: str) -> str:
    """minúsculas + leetspeak + sin tildes (sin quitar letras repetidas)."""
    t = texto.lower().translate(str.maketrans(LEET_DE, LEET_A))
    return "".join(c for c in unicodedata.normalize("NFD", t) if unicodedata.category(c) != "Mn")


def normalizar(texto: str) -> str:
    """base + letras repetidas colapsadas (gonorreaaaa → gonorea)."""
    return re.sub(r"(.)\1+", r"\1", base_texto(texto))


def compactar(texto: str) -> str:
    """solo letras: 'G o n o r r e a' o 'g.o.n.o.r.r.e.a' → 'gonorea'."""
    return re.sub(r"(.)\1+", r"\1", re.sub(r"[^a-zñ]", "", base_texto(texto)))


_largas = sorted({normalizar(w) for w in GROSERIAS}, key=len, reverse=True)
PATRON_GROSERIA = "(" + "|".join(_largas) + ")" + r"|\b(" + "|".join(GROSERIAS_CORTAS) + r")\b"
PATRON_GROSERIA_DOBLE = r"\b(" + "|".join(GROSERIAS_DOBLES) + r")\b"
# texto compactado: solo las largas (≥ 6 letras) y sin "marica/marico" ("toma rica" → "tomarica")
PATRON_GROSERIA_COMPACTA = "(" + "|".join(w for w in _largas if len(w) >= 6 and not w.startswith("maric")) + ")"


def detectar_groseria(texto: str) -> str:
    """Misma regla que aplica Spark, en Python (para pruebas y para verificar.py)."""
    for patron, t in ((PATRON_GROSERIA, normalizar(texto)), (PATRON_GROSERIA_DOBLE, base_texto(texto)),
                      (PATRON_GROSERIA_COMPACTA, compactar(texto))):
        m = re.search(patron, t)
        if m:
            return m.group(0)
    return ""


PATRON_ENLACE = r"(?i)(https?://|www\.|bit\.ly|\.xyz\b|\.com\b)"
PATRON_PROMO = r"(sigueme|suscribanse|suscribete a mi|compra seguidores|gana [0-9]+|gana dinero|plata facil)"


def preparar_carpetas():
    CHECKPOINTS.mkdir(parents=True, exist_ok=True)


class Color:
    R, G, Y, B, M, C, D, X, BOLD = ("\033[91m", "\033[92m", "\033[93m", "\033[94m", "\033[95m", "\033[96m",
                                    "\033[90m", "\033[0m", "\033[1m")
    ALERTA = "\033[41m\033[97m\033[1m"
    LOTE = "\033[44m\033[97m\033[1m"


os.system("")  # activa colores ANSI en la consola de Windows
