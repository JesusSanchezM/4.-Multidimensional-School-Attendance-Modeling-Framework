import sqlite3
import os
from pathlib import Path

# 1. Definimos las rutas (Paths)
BASE_DIR = Path(__file__).resolve().parent.parent # Regresa a la raíz del proyecto
DB_PATH = BASE_DIR / "data" / "processed" / "enigh_unificada.db"
SCHEMA_PATH = BASE_DIR / "src" / "schema.sql"

def init_db():
    """Crea la base de datos y sus tablas basándose en schema.sql"""
    
    # Asegurarnos que la carpeta 'processed' exista
    os.makedirs(DB_PATH.parent, exist_ok=True)

    # Conectamos a la base de datos (si no existe, se crea sola aquí)
    print(f"🔌 Conectando a: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Leemos el archivo schema.sql
    print(f"📖 Leyendo planos desde: {SCHEMA_PATH}")
    with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
        sql_script = f.read()

    # Ejecutamos el script SQL
    print("🏗️  Construyendo tablas...")
    cursor.executescript(sql_script)
    
    conn.commit()
    conn.close()
    print("✅ ¡Éxito! Base de datos creada con estructura vacía.")

if __name__ == "__main__":
    init_db()