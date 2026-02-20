import requests
import zipfile
import os
from config.paths import RAW_DIR

# URL base del INEGI
BASE_URL = "https://www.inegi.org.mx/contenidos/programas/enigh/nc/{year}/microdatos/enigh{year}_ns_{module}_csv.zip"

YEARS = [2018, 2024]

MODULES = [
    "viviendas",
    "concentradohogar",
    "gastospersona",
    "poblacion",
    "ingresos",
    "gastoshogar",
]

def download_and_extract(year, module):
    # Ajuste: A veces INEGI cambia ligeramente las URLs, pero esta estructura es la estándar reciente.
    url = BASE_URL.format(year=year, module=module)
    zip_name = f"{module}_{year}.zip"
    zip_path = RAW_DIR / zip_name

    print(f"⬇️  Descargando {module} {year}...")

    try:
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            print(f"⚠️  No disponible o error en URL: {url}")
            return

        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        print(f"📦 Extrayendo y renombrando...")
        
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            # Buscamos el CSV dentro del zip (ignorando manuales o txts)
            for file_name in zip_ref.namelist():
                if file_name.lower().endswith(".csv"):
                    # Extraemos el archivo original
                    zip_ref.extract(file_name, RAW_DIR)
                    
                    # Definimos el nombre original y el nuevo
                    original_path = RAW_DIR / file_name
                    new_name = f"{module}_{year}.csv"
                    new_path = RAW_DIR / new_name
                    
                    # Renombramos (movemos)
                    # Si el archivo original estaba en una subcarpeta del zip, lo sacamos a la raíz
                    original_path.replace(new_path)
                    
                    print(f"   ✅ Guardado como: {new_name}")

        # Limpieza: borrar el zip y carpetas vacías si quedaron
        zip_path.unlink()
        
    except Exception as e:
        print(f"❌ Error procesando {module} {year}: {e}")

if __name__ == "__main__":
    # Asegurar que el directorio exista
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    
    print("🚀 Iniciando descarga masiva corregida...\n")
    for year in YEARS:
        for module in MODULES:
            download_and_extract(year, module)
    print("\n✨ Descarga finalizada.") 