import sqlite3
import time
from pathlib import Path

# Configuración
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "processed" / "enigh_unificada.db"
SQL_SCRIPT_PATH = BASE_DIR / "src" / "transform.sql"

def run_transformation():
    print(f"🔌 Conectando a {DB_PATH.name}...")
    conn = sqlite3.connect(DB_PATH)
    
    print(f"📖 Leyendo script de transformación...")
    with open(SQL_SCRIPT_PATH, 'r', encoding='utf-8') as f:
        sql_script = f.read()
    
    print("⚙️  Ejecutando transformaciones (Esto puede tomar unos segundos)...")
    start_time = time.time()
    
    try:
        conn.executescript(sql_script)
        conn.commit()
        elapsed = time.time() - start_time
        print(f"✅ ¡Éxito! Tabla 'tabla_analitica_final' creada en {elapsed:.2f} segundos.")
        
        # Verificación rápida
        cursor = conn.execute("SELECT anio, COUNT(*) FROM tabla_analitica_final GROUP BY anio")
        print("\n📊 Resumen de filas generadas:")
        for row in cursor.fetchall():
            print(f"   Año {row[0]}: {row[1]:,} registros")
            
    except Exception as e:
        print(f"❌ Error durante la transformación: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_transformation()