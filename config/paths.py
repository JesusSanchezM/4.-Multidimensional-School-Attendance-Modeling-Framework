from pathlib import Path

# Root del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent

# Directorios principales
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"

OUTPUTS_DIR = BASE_DIR / "outputs"
FIGURES_DIR = OUTPUTS_DIR / "figures"
TABLES_DIR = OUTPUTS_DIR / "tables"

# Crear carpetas si no existen
for path in [
    RAW_DIR,
    INTERIM_DIR,
    PROCESSED_DIR,
    FIGURES_DIR,
    TABLES_DIR,
]:
    path.mkdir(parents=True, exist_ok=True)
