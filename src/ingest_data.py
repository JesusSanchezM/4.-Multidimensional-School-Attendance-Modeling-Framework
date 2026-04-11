import sqlite3
import pandas as pd
import sys
import glob # Necesario para buscar el archivo del censo
from pathlib import Path

# --- CONFIGURACIÓN ---
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "processed" / "enigh_unificada.db"
RAW_PATH = BASE_DIR / "data" / "raw"
CENSO_PATH = RAW_PATH / "censo_2020" # Ruta específica del Censo

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;") 
    return conn

def clean_and_prepare(df, year):
    """Limpieza básica común para todos los DataFrames de la ENIGH"""
    df = df.copy()
    
    df.columns = df.columns.str.lower().str.strip()
    df['anio'] = year
    
    if 'folioviv' in df.columns:
        df['folioviv'] = df['folioviv'].astype(str).str.split('.').str[0].str.zfill(10)
    
    if 'foliohog' in df.columns:
        df['foliohog'] = df['foliohog'].astype(str).str.split('.').str[0]
        
    if 'numren' in df.columns:
        df['numren'] = df['numren'].astype(str).str.split('.').str[0].str.zfill(2)

    if 'ubica_geo' in df.columns and 'entidad' not in df.columns:
        df['ubica_geo'] = df['ubica_geo'].astype(str).str.split('.').str[0].str.zfill(5)
        df['entidad'] = df['ubica_geo'].str[:2]
        
    return df

def process_table(year, csv_prefix, table_name):
    """Procesa los archivos estándar de la ENIGH (2018 y 2024)"""
    csv_name = f"{csv_prefix}_{year}.csv"
    csv_path = RAW_PATH / csv_name
    
    if not csv_path.exists():
        print(f"⚠️  No se encontró {csv_name}, saltando tabla {table_name}.")
        return

    print(f"   ↳ 📥 Cargando {csv_name} en tabla '{table_name}'...")
    
    conn = get_db_connection()
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    valid_sql_cols = [row[1] for row in cursor.fetchall()]
    
    chunk_size = 50000
    rows_inserted = 0
    
    try:
        for chunk in pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False):
            chunk = clean_and_prepare(chunk, year)
            cols_to_insert = [c for c in valid_sql_cols if c in chunk.columns]
            
            chunk[cols_to_insert].to_sql(table_name, conn, if_exists='append', index=False)
            rows_inserted += len(chunk)
            print(f"     ... {rows_inserted} filas procesadas.", end='\r')
            
        print(f"\n     ✅ {table_name}: {rows_inserted} registros completados.")
            
    except sqlite3.IntegrityError as e:
        print(f"\n     ❌ Error de Integridad en {table_name}: {e}")
    except Exception as e:
        print(f"\n     ❌ Error cargando {table_name}: Tipo: {type(e).__name__}, Detalle: {e}")
    finally:
        conn.close()

def process_censo():
    """Busca y procesa dinámicamente el archivo del Censo 2020"""
    print(f"\n📅 --- PROCESANDO CENSO 2020 ---")
    table_name = "censo_2020"
    
    # Busca cualquier CSV en la carpeta del Censo (ignorando mayúsculas/minúsculas)
    csv_files = glob.glob(str(CENSO_PATH / "**" / "*.[cC][sS][vV]"), recursive=True)
    
    if not csv_files:
        print("⚠️ No se encontró el archivo del Censo 2020 en la carpeta aislada.")
        return
        
    # Toma el archivo más pesado (por si hay manuales o metadatos)
    csv_path = max(csv_files, key=lambda x: Path(x).stat().st_size)
    print(f"   ↳ 📥 Cargando {Path(csv_path).name} en tabla '{table_name}'...")
    
    conn = get_db_connection()
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    valid_sql_cols = [row[1] for row in cursor.fetchall()]
    
    chunk_size = 50000
    rows_inserted = 0
    
    try:
        for chunk in pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False):
            # Limpieza básica para el Censo
            chunk.columns = chunk.columns.str.lower().str.strip()
            
            if 'id_viv' in chunk.columns:
                chunk['id_viv'] = chunk['id_viv'].astype(str).str.split('.').str[0]
            if 'id_persona' in chunk.columns:
                chunk['id_persona'] = chunk['id_persona'].astype(str).str.split('.').str[0]
            if 'ent' in chunk.columns:
                chunk['ent'] = chunk['ent'].astype(str).str.split('.').str[0].str.zfill(2)
                
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
    
    # 1. Procesar la ENIGH (2018 y 2024)
    years = [2018, 2024]
    tasks = {
        "viviendas": "viviendas",
        "concentradohogar": "hogares",
        "poblacion": "poblacion",
        "ingresos": "ingresos",
        "gastospersona": "gastos_persona",
        "gastoshogar": "gastos_hogar"
    }

    for year in years:
        print(f"\n📅 --- PROCESANDO ENIGH {year} ---")
        for csv_prefix, table_name in tasks.items():
            process_table(year, csv_prefix, table_name)
            
    # 2. Procesar el Censo 2020
    process_censo()
        
    print("\n🏁 Carga finalizada.")

if __name__ == "__main__":
    main()