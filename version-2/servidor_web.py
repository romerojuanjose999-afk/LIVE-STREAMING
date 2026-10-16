import json
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from config import BASE, PUERTO_WEB, TOPICS
from database import ConfiguracionBD
from database.estado import EstadoCompartido

INDEX = BASE / "web" / "index.html"
WEB = BASE / "web"
TIPOS = {".mp4": "video/mp4", ".webm": "video/webm", ".mov": "video/quicktime", ".mkv": "video/x-matroska",
         ".png": "image/png", ".jpg": "image/jpeg", ".css": "text/css", ".js": "text/javascript"}


ESTADO = None        # EstadoCompartido (MySQL): se crea al arrancar el servidor
URL_RED = None       # http://IP-del-PC:puerto (la que abren los compañeros en la misma Wi-Fi)


VIDEOS = (".mp4", ".webm", ".mov", ".mkv")


def buscar_video():
    """El video del directo: web/video.mp4 si existe; si no, el video más reciente de la carpeta web/."""
    if (WEB / "video.mp4").is_file():
        return "video.mp4"
    videos = [f for f in WEB.iterdir() if f.is_file() and f.suffix.lower() in VIDEOS]
    return max(videos, key=lambda f: f.stat().st_mtime).name if videos else None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def responder(self, cuerpo, tipo="application/json", codigo=200):
        data = cuerpo if isinstance(cuerpo, bytes) else json.dumps(cuerpo, ensure_ascii=False).encode()
        try:
            self.send_response(codigo)
            self.send_header("Content-Type", f"{tipo}; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            pass    # el navegador cerró la conexión (recargó o cerró la pestaña): es normal

    def do_GET(self):
        url = urlparse(self.path)
        if url.path in ("/", "/index.html"):
            return self.responder(INDEX.read_bytes(), "text/html")
        archivo = (WEB / unquote(url.path.lstrip("/"))).resolve()   # unquote: nombres con espacios
        if url.path == "/api/video":
            return self.responder({"video": buscar_video()})
        if url.path.startswith("/") and archivo.suffix.lower() in TIPOS and WEB.resolve() in archivo.parents:
            return self.enviar_archivo(archivo)
        if url.path == "/api/estado":
            try:
                return self.responder(self.estado_para_web(url))
            except Exception as e:      # MySQL ocupado o desconectado: la página reintenta sola
                print(f"\033[91m/api/estado falló: {e}\033[0m")
                return self.responder({"error": str(e)}, codigo=503)
        self.responder({"error": "no encontrado"}, codigo=404)

    def estado_para_web(self, url):
        """Todo lo que la página necesita en una sola respuesta (lo lee de MySQL)."""
        q = parse_qs(url.query)
        p, e = int(q.get("p", ["-1"])[0]), int(q.get("e", ["-1"])[0])
        prod, p_total = ESTADO.publicados_desde(p, 40)     # si el cliente recién llega (-1): los últimos
        evs, e_total = ESTADO.eventos_desde(e, 80)
        return {
            "modo": "spark",
            "fuentes": TOPICS,
            "productor": prod, "p": p_total,
            "eventos": evs, "e": e_total,
            "metricas": ESTADO.leer_valor("metricas", {}),
            "ventanas": ESTADO.leer_valor("ventanas", []),
            "baneados": ESTADO.baneados(),
            "control": ESTADO.leer_control(),
            "url_publica": URL_RED,
        }

    def enviar_archivo(self, ruta):
        """Sirve archivos de web/ (como video.mp4) con soporte de Range, para que el video se pueda adelantar y repetir."""
        if not ruta.is_file():
            self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers(); return
        total = ruta.stat().st_size
        inicio, fin = 0, total - 1
        rango = self.headers.get("Range", "")
        if rango.startswith("bytes="):
            a, _, b = rango[6:].split(",")[0].partition("-")
            inicio = int(a) if a else max(0, total - int(b or 0))
            fin = min(int(b), total - 1) if a and b else fin
        if inicio >= total or inicio > fin:
            self.send_response(416); self.send_header("Content-Range", f"bytes */{total}")
            self.send_header("Content-Length", "0"); self.end_headers(); return
        largo = fin - inicio + 1
        self.send_response(206 if rango else 200)
        self.send_header("Content-Type", TIPOS[ruta.suffix.lower()])
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(largo))
        if rango:
            self.send_header("Content-Range", f"bytes {inicio}-{fin}/{total}")
        self.end_headers()
        if self.command == "HEAD":
            return
        try:
            with open(ruta, "rb") as f:
                f.seek(inicio)
                while largo > 0:
                    bloque = f.read(min(1 << 20, largo))
                    if not bloque:
                        break
                    self.wfile.write(bloque)
                    largo -= len(bloque)
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
            pass    # el navegador cortó la descarga (normal al adelantar el video)

    def do_HEAD(self):
        self.do_GET()

    def do_POST(self):
        largo = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(largo) or b"{}")
        except ValueError:
            return self.responder({"error": "json inválido"}, codigo=400)
        ruta = urlparse(self.path).path
        if ruta == "/api/control":
            ctrl = ESTADO.leer_control()
            for k in ("tasa", "toxicos", "pausado"):
                if k in body:
                    ctrl[k] = body[k]
            ESTADO.guardar_control(ctrl)                     # tabla control
            return self.responder(ctrl)
        if ruta == "/api/comando":
            ESTADO.agregar_comando(body)                     # tabla comandos (la leen los productores)
            return self.responder({"ok": True})
        if ruta == "/api/desbanear":
            ESTADO.desbanear(str(body.get("usuario", "")))   # DELETE en la tabla baneados
            return self.responder({"ok": True})
        self.responder({"error": "no encontrado"}, codigo=404)


def ip_local():
    """IP del PC en la red (Wi-Fi/LAN) para que otro equipo de la misma red se conecte."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


if __name__ == "__main__":
    ESTADO = EstadoCompartido(ConfiguracionBD.desde_entorno())
    V, C, D, X = "\033[92m\033[1m", "\033[96m", "\033[90m", "\033[0m"
    print(f"{V}$ python servidor_web.py{X}")
    print(f"  Este PC:          {C}http://localhost:{PUERTO_WEB}{X}")
    URL_RED = f"http://{ip_local()}:{PUERTO_WEB}"
    print(f"  Misma red Wi-Fi:  {C}{URL_RED}{X}   ")
    try:
        ThreadingHTTPServer(("0.0.0.0", PUERTO_WEB), Handler).serve_forever()
    except KeyboardInterrupt:
        print("\nServidor detenido.")
