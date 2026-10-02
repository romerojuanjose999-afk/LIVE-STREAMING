import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from config import (BANEADOS, BASE, COMANDOS, CONTROL, CONTROL_DEFECTO, DESBANEOS, EVENTOS, METRICAS,
                    PRODUCTOR_LOG, PUERTO_WEB, VENTANAS, agregar_jsonl, escribir_json, leer_json,
                    leer_jsonl_desde, preparar_carpetas)

INDEX = BASE / "web" / "index.html"
WEB = BASE / "web"
TIPOS = {".mp4": "video/mp4", ".webm": "video/webm", ".mov": "video/quicktime", ".mkv": "video/x-matroska",
         ".png": "image/png", ".jpg": "image/jpeg", ".css": "text/css", ".js": "text/javascript"}


def ultimos(ruta, desde, maximo):
    """Si el cliente recién llega (desde=-1) solo le mandamos las últimas `maximo` líneas."""
    if desde < 0:
        _, total = leer_jsonl_desde(ruta, 0)
        desde = max(0, total - maximo)
    return leer_jsonl_desde(ruta, desde)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def responder(self, cuerpo, tipo="application/json", codigo=200):
        data = cuerpo if isinstance(cuerpo, bytes) else json.dumps(cuerpo, ensure_ascii=False).encode()
        self.send_response(codigo)
        self.send_header("Content-Type", f"{tipo}; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path in ("/", "/index.html"):
            return self.responder(INDEX.read_bytes(), "text/html")
        archivo = (WEB / url.path.lstrip("/")).resolve()
        if url.path.startswith("/") and archivo.suffix.lower() in TIPOS and WEB.resolve() in archivo.parents:
            return self.enviar_archivo(archivo)
        if url.path == "/api/estado":
            q = parse_qs(url.query)
            p, e = int(q.get("p", ["-1"])[0]), int(q.get("e", ["-1"])[0])
            prod, p_total = ultimos(PRODUCTOR_LOG, p, 40)
            evs, e_total = ultimos(EVENTOS, e, 80)
            return self.responder({
                "modo": "spark",
                "productor": prod, "p": p_total,
                "eventos": evs, "e": e_total,
                "metricas": leer_json(METRICAS, {}),
                "ventanas": leer_json(VENTANAS, []),
                "baneados": leer_json(BANEADOS, {"usuarios": {}})["usuarios"],
                "control": {**CONTROL_DEFECTO, **leer_json(CONTROL, {})},
            })
        self.responder({"error": "no encontrado"}, codigo=404)

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
            ctrl = {**CONTROL_DEFECTO, **leer_json(CONTROL, {})}
            for k in ("tasa", "toxicos", "pausado"):
                if k in body:
                    ctrl[k] = body[k]
            escribir_json(CONTROL, ctrl)
            return self.responder(ctrl)
        if ruta == "/api/comando":
            agregar_jsonl(COMANDOS, [body])
            return self.responder({"ok": True})
        if ruta == "/api/desbanear":
            agregar_jsonl(DESBANEOS, [body])
            return self.responder({"ok": True})
        self.responder({"error": "no encontrado"}, codigo=404)


if __name__ == "__main__":
    preparar_carpetas()
    print(f"\033[92m\033[1m$ python servidor_web.py\033[0m  →  abre  \033[96mhttp://localhost:{PUERTO_WEB}\033[0m")
    ThreadingHTTPServer(("0.0.0.0", PUERTO_WEB), Handler).serve_forever()
