-- Habilitar foreign keys
PRAGMA foreign_keys = ON;

-- 1. TABLA VIVIENDAS
-- Fuente: viviendas_20xx.csv
DROP TABLE IF EXISTS viviendas;
CREATE TABLE viviendas (
    folioviv    TEXT NOT NULL,
    anio        INTEGER NOT NULL,
    ubica_geo   TEXT, -- Clave geográfica
    entidad     TEXT, -- Primeros 2 dígitos de ubica_geo
    tam_loc     TEXT, -- Tamaño de localidad
    est_dis     TEXT, -- Estrato de diseño
    upm         TEXT, -- Unidad primaria de muestreo
    
    PRIMARY KEY (folioviv, anio)
);

-- 2. TABLA HOGARES
-- Fuente: hogares_20xx.csv
DROP TABLE IF EXISTS hogares;
CREATE TABLE hogares (
    folioviv    TEXT NOT NULL,
    foliohog    TEXT NOT NULL,
    anio        INTEGER NOT NULL,
    ing_cor     REAL, -- Ingreso corriente
    tot_integ   INTEGER,
    educa_jefe  TEXT,
    factor      REAL, -- IMPORTANTE: El factor de expansión suele estar aquí
    
    PRIMARY KEY (folioviv, foliohog, anio),
    FOREIGN KEY (folioviv, anio) REFERENCES viviendas(folioviv, anio)
);

-- 3. TABLA POBLACION
-- Fuente: poblacion_20xx.csv
DROP TABLE IF EXISTS poblacion;
CREATE TABLE poblacion (
    folioviv    TEXT NOT NULL,
    foliohog    TEXT NOT NULL,
    numren      TEXT NOT NULL,
    anio        INTEGER NOT NULL,
    
    parentesco  INTEGER,
    sexo        INTEGER,
    edad        INTEGER,
    etnia       INTEGER, -- Variable hablaind o etnia
    edo_conyug  INTEGER,
    
    -- Variables educativas clave
    asis_esc    INTEGER,
    nivelaprob  INTEGER,
    gradoaprob  INTEGER,
    tipoesc     INTEGER,
    tiene_b     INTEGER,
    otorg_b     TEXT,
    
    -- Discapacidad
    discapacidad_ver TEXT, 
    
    PRIMARY KEY (folioviv, foliohog, numren, anio),
    FOREIGN KEY (folioviv, foliohog, anio) REFERENCES hogares(folioviv, foliohog, anio)
);

-- 4. TABLA INGRESOS
-- Fuente: ingresos_20xx.csv
DROP TABLE IF EXISTS ingresos;
CREATE TABLE ingresos (
    folioviv    TEXT NOT NULL,
    foliohog    TEXT NOT NULL,
    numren      TEXT NOT NULL,
    anio        INTEGER NOT NULL,
    clave       TEXT NOT NULL,
    ing_tri     REAL,
    
    FOREIGN KEY (folioviv, foliohog, numren, anio) REFERENCES poblacion(folioviv, foliohog, numren, anio)
);

-- 5. TABLA GASTOS PERSONA
-- Fuente: gastospersona_20xx.csv
DROP TABLE IF EXISTS gastos_persona;
CREATE TABLE gastos_persona (
    folioviv    TEXT NOT NULL,
    foliohog    TEXT NOT NULL,
    numren      TEXT NOT NULL,
    anio        INTEGER NOT NULL,
    clave       TEXT, 
    gas_nm_tri  REAL,
    
    FOREIGN KEY (folioviv, foliohog, numren, anio) REFERENCES poblacion(folioviv, foliohog, numren, anio)
);

-- 6. TABLA GASTOS HOGAR (NUEVA)
-- Fuente: gastoshogar_20xx.csv (Para internet, luz, servicios)
DROP TABLE IF EXISTS gastos_hogar;
CREATE TABLE gastos_hogar (
    folioviv    TEXT NOT NULL,
    foliohog    TEXT NOT NULL,
    anio        INTEGER NOT NULL,
    clave       TEXT,
    gasto_tri   REAL,
    gas_nm_tri  REAL,
    
    FOREIGN KEY (folioviv, foliohog, anio) REFERENCES hogares(folioviv, foliohog, anio)
);

-- ÍNDICES
CREATE INDEX idx_viviendas_ubica ON viviendas(ubica_geo);
CREATE INDEX idx_poblacion_edad ON poblacion(edad);
CREATE INDEX idx_poblacion_asis ON poblacion(asis_esc);
CREATE INDEX idx_ingresos_clave ON ingresos(clave);
CREATE INDEX idx_gastoshogar_clave ON gastos_hogar(clave);