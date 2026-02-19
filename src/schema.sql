-- 1. Desactivar llaves foráneas temporalmente para permitir el DROP de tablas relacionadas
PRAGMA foreign_keys = OFF;

-- 2. Limpieza total en orden inverso a la jerarquía
DROP TABLE IF EXISTS gastos_hogar;
DROP TABLE IF EXISTS gastos_persona;
DROP TABLE IF EXISTS ingresos;
DROP TABLE IF EXISTS poblacion;
DROP TABLE IF EXISTS hogares;
DROP TABLE IF EXISTS viviendas;

-- 3. Reactivar llaves foráneas
PRAGMA foreign_keys = ON;

-- ============================================================
-- ESTRUCTURA RELACIONAL ENIGH (TESIS)
-- ============================================================

-- A. TABLA VIVIENDAS
CREATE TABLE viviendas (
    folioviv    TEXT NOT NULL,
    anio        INTEGER NOT NULL,
    ubica_geo   TEXT,
    entidad     TEXT,
    tam_loc     TEXT,
    est_dis     TEXT,
    upm         TEXT,
    factor      REAL,
    PRIMARY KEY (folioviv, anio)
);

-- B. TABLA HOGARES
CREATE TABLE hogares (
    folioviv    TEXT NOT NULL,
    foliohog    TEXT NOT NULL,
    anio        INTEGER NOT NULL,
    ing_cor     REAL,
    tot_integ   INTEGER,
    educa_jefe  TEXT,
    factor      REAL,
    est_socio   INTEGER,
    PRIMARY KEY (folioviv, foliohog, anio),
    FOREIGN KEY (folioviv, anio) REFERENCES viviendas(folioviv, anio)
);

-- C. TABLA POBLACION (Con todas las variables de tiempo y discapacidad)
CREATE TABLE poblacion (
    folioviv    TEXT NOT NULL,
    foliohog    TEXT NOT NULL,
    numren      TEXT NOT NULL,
    anio        INTEGER NOT NULL,
    parentesco  INTEGER,
    sexo        INTEGER,
    edad        INTEGER,
    etnia       INTEGER, 
    hablaind    INTEGER,
    edo_conyug  INTEGER,
    asis_esc    INTEGER,
    nivelaprob  INTEGER,
    gradoaprob  INTEGER,
    tipoesc     INTEGER,
    tiene_b     INTEGER,
    otorg_b     TEXT,
    no_asisb    TEXT, -- Variable crítica para descriptivos
    trabajo_mp  INTEGER,
    -- Variables de tiempo para horas_no_estudio
    hor_1 INTEGER, min_1 INTEGER, hor_2 INTEGER, min_2 INTEGER,
    hor_3 INTEGER, min_3 INTEGER, hor_4 INTEGER, min_4 INTEGER,
    hor_5 INTEGER, min_5 INTEGER, hor_6 INTEGER, min_6 INTEGER,
    hor_7 INTEGER, min_7 INTEGER,
    -- Discapacidad (2018 y 2024)
    disc1 INTEGER, disc_ver TEXT, disc_oir TEXT, disc_brazo TEXT,
    disc_camin TEXT, disc_apren TEXT, disc_vest TEXT, disc_habla TEXT, disc_acti TEXT,
    PRIMARY KEY (folioviv, foliohog, numren, anio),
    FOREIGN KEY (folioviv, foliohog, anio) REFERENCES hogares(folioviv, foliohog, anio)
);

-- D. TABLA INGRESOS
CREATE TABLE ingresos (
    folioviv    TEXT NOT NULL,
    foliohog    TEXT NOT NULL,
    numren      TEXT NOT NULL,
    anio        INTEGER NOT NULL,
    clave       TEXT NOT NULL,
    ing_tri     REAL,
    FOREIGN KEY (folioviv, foliohog, numren, anio) REFERENCES poblacion(folioviv, foliohog, numren, anio)
);

-- E. TABLA GASTOS PERSONA
CREATE TABLE gastos_persona (
    folioviv    TEXT NOT NULL,
    foliohog    TEXT NOT NULL,
    numren      TEXT NOT NULL,
    anio        INTEGER NOT NULL,
    clave       TEXT, 
    gas_nm_tri  REAL,
    FOREIGN KEY (folioviv, foliohog, numren, anio) REFERENCES poblacion(folioviv, foliohog, numren, anio)
);

-- F. TABLA GASTOS HOGAR
CREATE TABLE gastos_hogar (
    folioviv    TEXT NOT NULL,
    foliohog    TEXT NOT NULL,
    anio        INTEGER NOT NULL,
    clave       TEXT,
    gasto_tri   REAL,
    gas_nm_tri  REAL,
    FOREIGN KEY (folioviv, foliohog, anio) REFERENCES hogares(folioviv, foliohog, anio)
);

-- ÍNDICES PARA VELOCIDAD
CREATE INDEX idx_viviendas_ubica ON viviendas(ubica_geo);
CREATE INDEX idx_poblacion_edad ON poblacion(edad);
CREATE INDEX idx_poblacion_asis ON poblacion(asis_esc);