import sys
import subprocess
import os
import requests
import glob
import pandas as pd
from tqdm import tqdm
import warnings

warnings.filterwarnings('ignore')

# --- 1. CONFIGURACIÓN DE RUTAS ---
RAW_DIR = os.path.join("data", "raw")
CENSO_DIR = os.path.join(RAW_DIR, "censo_2020")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CENSO_DIR, exist_ok=True)

URL_CENSO = "https://www.inegi.org.mx/contenidos/programas/ccpv/2020/microdatos/Censo2020_CA_eum_csv.zip"
ZIP_FILE = os.path.join(RAW_DIR, "Censo2020_CA_eum_csv.zip")
COLS_A_EXTRAER = [
    'id_viv', 'id_persona', 'ent', 'edad', 'ingtrmen', 'factor', 
    'cobertura', 'asisten', 'nivacad', 'escolari', 'conact', 'sittra'
]

# --- 2. FUNCIONES DEL PIPELINE ---
def descargar_robusto(url, destino):
    respuesta_head = requests.head(url)
    tamanio_total = int(respuesta_head.headers.get('content-length', 0))
   
    tamanio_descargado = 0
    modo_escritura = 'wb'
   
    if os.path.exists(destino):
        tamanio_descargado = os.path.getsize(destino)
        if tamanio_descargado >= tamanio_total:
            print("✅ El archivo ZIP ya está descargado al 100%.")
            return True
        else:
            print(f"⚠️ Archivo incompleto. Reanudando descarga desde {tamanio_descargado / (1024*1024):.1f} MB...")
            modo_escritura = 'ab'
    else:
        print("⬇️ Iniciando descarga nueva del Censo...")

    headers = {'Range': f'bytes={tamanio_descargado}-'}
   
    try:
        with requests.get(url, headers=headers, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(destino, modo_escritura) as f, tqdm(
                desc="Descargando CPV", initial=tamanio_descargado, total=tamanio_total,
                unit='iB', unit_scale=True, unit_divisor=1024,
            ) as barra:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        barra.update(len(chunk))
        return True
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Error de red: {e}")
        return False

def procesar_censo():
    if not descargar_robusto(URL_CENSO, ZIP_FILE):
        return None
   
    print(f"\n📦 Extrayendo con el motor nativo de tu Mac en {CENSO_DIR}...")
    try:
        subprocess.run(["unzip", "-q", "-o", ZIP_FILE, "-d", CENSO_DIR], check=True)
        print("✅ Extracción completada con éxito.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al extraer usando el sistema operativo: {e}")
        return None

    print("\n🔍 Buscando el conjunto de datos principal del Censo...")
    
    # CAMBIO AQUÍ: Búsqueda que ignora mayúsculas/minúsculas
    archivos_csv = glob.glob(os.path.join(CENSO_DIR, '**', '*.[cC][sS][vV]'), recursive=True)
    
    # CAMBIO AQUÍ: Modo detective si no encuentra CSVs
    if not archivos_csv:
        print("❌ No se encontraron archivos CSV. 🕵️‍♂️ Revisando qué archivos SÍ se extrajeron:")
        for root, dirs, files in os.walk(CENSO_DIR):
            for file in files:
                print(f"   📄 Encontrado: {os.path.join(root, file)}")
        return None
        
    csv_path = max(archivos_csv, key=os.path.getsize)
    print(f"✅ Archivo objetivo localizado: {os.path.basename(csv_path)} ({(os.path.getsize(csv_path) / (1024*1024)):.1f} MB)")

    print("\n📊 Verificando los datos extraídos (leyendo una muestra de 5 filas)...")
    try:
        cols_lower = [c.lower() for c in COLS_A_EXTRAER]
        df_header = pd.read_csv(csv_path, nrows=1, low_memory=False)
        columnas_reales = [c.lower() for c in df_header.columns]
        columnas_a_usar = [c for c in cols_lower if c in columnas_reales]
        
        if len(columnas_a_usar) < len(cols_lower):
            faltantes = set(cols_lower) - set(columnas_a_usar)
            print(f"⚠️ Nota: No se encontraron estas columnas en el Censo: {faltantes}")

        df = pd.read_csv(
            csv_path,
            usecols=lambda x: x.lower() in columnas_a_usar,
            nrows=5,
            low_memory=False
        )
       
        print("\n🚀 ¡Muestra de los datos en raw lista!\n")
        print(df)
        return csv_path
        
    except Exception as e:
         print(f"\n⚠️ Error al leer el CSV: {e}")
         return csv_path

if __name__ == "__main__":
    print("=== INICIANDO PIPELINE DE DESCARGA Y EXTRACCIÓN ===")
    ruta_csv = procesar_censo()
    if ruta_csv:
        print("\n✅ Proceso Finalizado. El archivo está listo para tu base de datos SQLite.")