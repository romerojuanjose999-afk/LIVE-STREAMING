#Muestra en consola lo que Spark ha guardado en MySQL 
from config import Color as C
from database import ConexionMySQL, ConfiguracionBD

CONSULTAS = {
    "Resumen por acción": """
        SELECT accion, COUNT(*) AS total FROM mensajes GROUP BY accion ORDER BY total DESC""",
    "Mensajes por productor (topic de Kafka)": """
        SELECT fuente, tipo, COUNT(*) AS total, COALESCE(SUM(monto), 0) AS monto_cop
        FROM mensajes GROUP BY fuente, tipo ORDER BY fuente""",
    "Expulsados en este momento (tabla baneados)": """
        SELECT usuario, motivo, hora FROM baneados ORDER BY hora DESC LIMIT 10""",
    "Estado compartido (cuántas filas tiene cada tabla)": """
        SELECT 'publicados' AS tabla, COUNT(*) AS filas FROM publicados
        UNION ALL SELECT 'comandos', COUNT(*) FROM comandos
        UNION ALL SELECT 'eventos_web', COUNT(*) FROM eventos_web
        UNION ALL SELECT 'baneados', COUNT(*) FROM baneados""",
    "Últimas 10 expulsiones": """
        SELECT usuario, fuente, motivo, DATE_FORMAT(fecha, '%H:%i:%s') AS hora
        FROM expulsiones ORDER BY id DESC LIMIT 10""",
    "Últimos 5 lotes insertados": """
        SELECT lote_id, filas, aprobados, eliminados, expulsados, superchats, duracion_ms,
               DATE_FORMAT(fecha_proceso, '%H:%i:%s') AS hora
        FROM lotes ORDER BY id DESC LIMIT 5""",
}


def imprimir_tabla(filas):
    if not filas:
        print(f"  {C.D}(sin datos todavía){C.X}")
        return
    columnas = list(filas[0])
    anchos = [max(len(str(c)), *(len(str(f[c])) for f in filas)) for c in columnas]
    print("  " + C.BOLD + "  ".join(str(c).ljust(a) for c, a in zip(columnas, anchos)) + C.X)
    for f in filas:
        print("  " + "  ".join(str(f[c]).ljust(a) for c, a in zip(columnas, anchos)))


if __name__ == "__main__":
    config = ConfiguracionBD.desde_entorno()
    with ConexionMySQL(config) as bd:
        bd.crear_tablas()
        print(f"\n{C.G}{C.BOLD}Base de datos: {config}{C.X}")
        for titulo, sql in CONSULTAS.items():
            print(f"\n{C.C}■ {titulo}{C.X}")
            imprimir_tabla(bd.consultar(sql))
        print()
