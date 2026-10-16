import json, os, random, time
from datetime import datetime

from config import (MENSAJES_LAMBON, PROB_LAMBON, MIN_ACTIVOS, PREFIJOS, SUFIJOS, BANEADOS, COMANDOS, CONTROL, CONTROL_DEFECTO, ESPECTADOR, GROSERIAS_GENERADAS,
                    MENSAJES_NORMALES, MENSAJES_SPAM, PLANTILLAS_TOXICAS, PRODUCTOR_LOG, STREAM_DIR, TMP_DIR,
                    USUARIOS, Color as C, agregar_jsonl, escribir_json, leer_json, leer_jsonl_desde,
                    preparar_carpetas)

INTERVALO = 0.5  # segundos entre publicaciones


def ahora_iso():
    return datetime.now().isoformat(timespec="milliseconds")


def generar_texto(tipo, ultimo=""):
    if tipo == "groseria":
        return random.choice(PLANTILLAS_TOXICAS).format(w=random.choice(GROSERIAS_GENERADAS))
    if tipo == "spam":
        return random.choice(MENSAJES_SPAM)
    lista = MENSAJES_LAMBON if tipo == "lambon" or random.random() * 100 < PROB_LAMBON else MENSAJES_NORMALES
    texto = random.choice(lista)
    while texto == ultimo:
        texto = random.choice(lista)
    return texto


def publicar(mensajes, n_archivo):
    nombre = f"chat_{n_archivo:06d}.json"
    tmp = TMP_DIR / nombre
    with open(tmp, "w", encoding="utf-8") as f:
        for m in mensajes:
            f.write(json.dumps(m, ensure_ascii=False) + "\n")
    os.replace(tmp, STREAM_DIR / nombre)
    return nombre


def main():
    preparar_carpetas()
    if not CONTROL.exists():
        escribir_json(CONTROL, CONTROL_DEFECTO)

    offset = sum(1 for _ in open(PRODUCTOR_LOG, encoding="utf-8")) if PRODUCTOR_LOG.exists() else 0
    n_archivo = len(list(STREAM_DIR.glob("chat_*.json")))
    _, comandos_leidos = leer_jsonl_desde(COMANDOS, 0)   # ignora comandos viejos
    ultimo_texto, baneados_vistos, pausado_antes = {}, set(), None
    usuarios = list(USUARIOS)

    print(f"{C.G}{C.BOLD}$ python productor.py  →  fuente de Spark: {STREAM_DIR}{C.X}")
    print(f"{C.D}{len(USUARIOS)} usuarios en el chat · publicando un archivo cada {INTERVALO}s{C.X}\n")

    while True:
        ctrl = {**CONTROL_DEFECTO, **leer_json(CONTROL, {})}
        baneados = set(leer_json(BANEADOS, {}).get("usuarios", {}).keys())

        for u in sorted(baneados - baneados_vistos):
            print(f"{C.R}{C.BOLD}[{ahora_iso()[11:]}] ✖ @{u} fue EXPULSADO por el moderador → ya no puede escribir{C.X}")
        baneados_vistos = baneados
        activos = [u for u in usuarios if u not in baneados]
        for _ in range(min(2, MIN_ACTIVOS - len(activos)) if not ctrl["pausado"] else 0):
            nuevo = random.choice(PREFIJOS) + random.choice(SUFIJOS)
            if nuevo in usuarios:
                nuevo += str(random.randint(10, 999))
            if nuevo not in usuarios:
                usuarios.append(nuevo); activos.append(nuevo)
                print(f"{C.G}[{ahora_iso()[11:]}] 👋 @{nuevo} entró al directo (usuarios activos: {len(activos)}){C.X}")

        if ctrl["pausado"] != pausado_antes:
            print(f"{C.Y}{'⏸ Productor PAUSADO' if ctrl['pausado'] else '▶ Productor ACTIVO'} "
                  f"(tasa {ctrl['tasa']} msg/s · tóxicos {ctrl['toxicos']}%){C.X}")
            pausado_antes = ctrl["pausado"]

        # 1) mensajes forzados desde el frontend (inyecciones y el espectador)
        pendientes = []
        nuevos, comandos_leidos = leer_jsonl_desde(COMANDOS, comandos_leidos)
        for cmd in nuevos:
            if cmd.get("tipo") == "espectador":
                if ESPECTADOR not in baneados:
                    pendientes.append((ESPECTADOR, cmd.get("mensaje", "")[:160]))
            elif activos:
                pendientes.append((random.choice(activos), generar_texto(cmd.get("tipo"))))

        # 2) mensajes automáticos de los usuarios
        if not ctrl["pausado"] and activos:
            esperado = ctrl["tasa"] * INTERVALO
            n = int(esperado) + (1 if random.random() < esperado % 1 else 0)
            for _ in range(n):
                u = random.choice(activos)
                r = random.random() * 100
                tipo = "groseria" if r < ctrl["toxicos"] * .55 else "spam" if r < ctrl["toxicos"] else "normal"
                pendientes.append((u, generar_texto(tipo, ultimo_texto.get(u, ""))))

        if pendientes:
            mensajes = []
            for u, texto in pendientes:
                mensajes.append({"offset": offset, "usuario": u, "mensaje": texto, "ts": ahora_iso()})
                ultimo_texto[u] = texto
                offset += 1
            n_archivo += 1
            archivo = publicar(mensajes, n_archivo)
            agregar_jsonl(PRODUCTOR_LOG, [{**m, "archivo": archivo} for m in mensajes])
            print(f"{C.D}── {archivo} ({len(mensajes)} msg) → data/stream/{C.X}")
            for m in mensajes:
                print(f"{C.D}[{m['ts'][11:]}]{C.X} {C.B}PRODUCE →{C.X} {C.M}offset:{m['offset']:05d}{C.X} "
                      f"{C.BOLD}@{m['usuario']}{C.X} {C.D}\"{m['mensaje']}\"{C.X}")
        time.sleep(INTERVALO)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{C.Y}Productor detenido.{C.X}")
