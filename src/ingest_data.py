import sqlite3
import pandas as pd
import sys
from pathlib import Path

# --- CONFIGURACIÓN ---
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "processed" / "enigh_unificada.db"
RAW_PATH = BASE_DIR / "data" / "raw"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;") 
    return conn

def clean_and_prepare(df, year):
    """Limpieza básica común para todos los DataFrames"""
    # 1. Normalizar a minúsculas
    df.columns = df.columns.str.lower().str.strip()
    
    # 2. Agregar año
    df['anio'] = year
    
    # 3. Formatear folioviv como texto (clave)
    if 'folioviv' in df.columns:
        df['folioviv'] = df['folioviv'].astype(str).str.zfill(10)
    
    # 4. Formatear foliohog y numren como texto (clave)
    if 'foliohog' in df.columns:
        df['foliohog'] = df['foliohog'].astype(str) # A veces es '1', a veces '01'
    if 'numren' in df.columns:
        df['numren'] = df['numren'].astype(str).str.zfill(2)

    # 5. Generar entidad desde ubica_geo si existe
    if 'ubica_geo' in df.columns and 'entidad' not in df.columns:
        df['ubica_geo'] = df['ubica_geo'].astype(str).str.zfill(5)
        df['entidad'] = df['ubica_geo'].str[:2]
        
    return df

def process_table(year, csv_prefix, table_name):
    """
    Función Genérica Maestra:
    Busca el archivo {csv_prefix}_{year}.csv y lo carga en la tabla SQL {table_name}.
    """
    csv_name = f"{csv_prefix}_{year}.csv"
    csv_path = RAW_PATH / csv_name
    
    if not csv_path.exists():
        print(f"⚠️  No se encontró {csv_name}, saltando tabla {table_name}.")
        return

    print(f"   ↳ 📥 Cargando {csv_name} en tabla '{table_name}'...")
    
    conn = get_db_connection()
    
    # Obtenemos las columnas que REALMENTE existen en la tabla SQL
    # Esto evita errores si el CSV trae columnas basura
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    valid_sql_cols = [row[1] for row in cursor.fetchall()]
    
    chunk_size = 50000
    rows_inserted = 0
    
    try:
        # Leemos en chunks para no saturar memoria RAM
        for chunk in pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False):
            chunk = clean_and_prepare(chunk, year)
            
            # Filtramos: Solo insertamos las columnas que existen en el Schema SQL
            cols_to_insert = [c for c in valid_sql_cols if c in chunk.columns]
            
            chunk[cols_to_insert].to_sql(table_name, conn, if_exists='append', index=False)
            rows_inserted += len(chunk)
            print(f"     ... {rows_inserted} filas procesadas.", end='\r')
            
        print(f"\n     ✅ {table_name}: {rows_inserted} registros completados.")
            
    except Exception as e:
        print(f"\n     ❌ Error cargando {table_name}: {e}")
    finally:
        conn.close()

def main():
    print("🚀 Iniciando carga de datos RELACIONAL...")
    years = [2018, 2024]
    
    # Diccionario: { Prefijo_CSV : Nombre_Tabla_SQL }
    # El orden importa: Primero padres (Viviendas), luego hijos (Hogares), luego nietos (Poblacion)
    tasks = {
        "viviendas": "viviendas",
        "hogares": "hogares",
        "poblacion": "poblacion",
        "ingresos": "ingresos",
        "gastospersona": "gastos_persona",
        "gastoshogar": "gastos_hogar"
    }

    for year in years:
        print(f"\n📅 --- PROCESANDO AÑO {year} ---")
        for csv_prefix, table_name in tasks.items():
            process_table(year, csv_prefix, table_name)
        
    print("\n🏁 Carga finalizada.")

if __name__ == "__main__":
    main()