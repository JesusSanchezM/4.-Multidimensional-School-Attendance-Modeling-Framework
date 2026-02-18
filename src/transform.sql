-- =========================================================================
-- TRANSFORMACIÓN DE DATOS PARA TESIS (OPTIMIZADO)
-- =========================================================================

-- 1. LIMPIEZA
DROP VIEW IF EXISTS v_hogares_geo;
DROP VIEW IF EXISTS v_ingresos_deflactados;
DROP VIEW IF EXISTS v_educacion_normalizada;
DROP VIEW IF EXISTS v_hogares_internet; -- Nueva vista auxiliar
DROP TABLE IF EXISTS tabla_analitica_final;

-- 2. VISTA MAESTRA DE HOGARES
CREATE VIEW v_hogares_geo AS
SELECT 
    h.folioviv, h.foliohog, h.anio, h.factor, h.tot_integ,
    v.ubica_geo, v.entidad, v.tam_loc, v.est_dis, v.upm,
    CASE WHEN v.tam_loc = '4' THEN 'Rural' ELSE 'Urbano' END AS ambito,
    CASE 
        WHEN v.entidad IN ('02','08','05','19','26','28') THEN 'Norte'
        WHEN v.entidad IN ('03','25','18','10','32') THEN 'Norte-occidente'
        WHEN v.entidad IN ('14','01','06','16','24') THEN 'Centro-norte'
        WHEN v.entidad IN ('11','22','13','15','17','29','09','21') THEN 'Centro'
        WHEN v.entidad IN ('12','20','07','30','27','04','31','23') THEN 'Sur'
        ELSE 'Desconocido'
    END AS region
FROM hogares h
JOIN viviendas v ON h.folioviv = v.folioviv AND h.anio = v.anio;

-- 3. VISTA AUXILIAR DE INTERNET (Aquí está la optimización)
-- Calculamos quién tiene internet UNA sola vez
CREATE VIEW v_hogares_internet AS
SELECT DISTINCT folioviv, foliohog, anio, 1 AS tiene_internet
FROM gastos_hogar
WHERE clave IN ('R008','R010','3301','083401');

-- 4. INGRESO TOTAL INDIVIDUAL (Deflactado)
CREATE VIEW v_ingresos_deflactados AS
SELECT 
    p.folioviv, p.foliohog, p.numren, p.anio,
    COALESCE(SUM(CASE WHEN p.anio = 2024 THEN i.ing_tri * 0.748816 ELSE i.ing_tri END), 0) AS ingreso_monetario,
    COALESCE(SUM(CASE WHEN i.clave = 'P001' THEN (CASE WHEN p.anio = 2024 THEN i.ing_tri * 0.748816 ELSE i.ing_tri END) ELSE 0 END), 0) AS ingreso_laboral
FROM poblacion p
LEFT JOIN ingresos i 
    ON p.folioviv = i.folioviv AND p.foliohog = i.foliohog 
    AND p.numren = i.numren AND p.anio = i.anio
GROUP BY p.folioviv, p.foliohog, p.numren, p.anio;

-- 5. EDUCACIÓN (Normalización)
CREATE VIEW v_educacion_normalizada AS
SELECT 
    folioviv, foliohog, numren, anio, parentesco, edad, sexo,
    asis_esc,
    etnia, -- Aseguramos que pase la etnia
    edo_conyug,
    nivelaprob, gradoaprob,
    CASE
        WHEN nivelaprob IN (8,9) THEN 5
        WHEN nivelaprob IN (5,6,7) AND gradoaprob >= 4 THEN 4
        WHEN nivelaprob = 4 AND gradoaprob = 3 THEN 3
        WHEN nivelaprob = 3 AND gradoaprob = 3 THEN 2
        WHEN nivelaprob = 2 AND gradoaprob = 6 THEN 1
        ELSE 0 
    END AS escolaridad_num
FROM poblacion;

-- 6. RATIOS SALARIALES
DROP TABLE IF EXISTS ratios_estatales;
CREATE TABLE ratios_estatales AS
WITH salarios_promedio AS (
    SELECT 
        v.anio, v.entidad,
        AVG(CASE WHEN e.escolaridad_num = 2 THEN i.ingreso_laboral END) as w_secundaria,
        AVG(CASE WHEN e.escolaridad_num = 3 THEN i.ingreso_laboral END) as w_prepa,
        AVG(CASE WHEN e.escolaridad_num >= 4 THEN i.ingreso_laboral END) as w_lic
    FROM v_ingresos_deflactados i
    JOIN v_educacion_normalizada e 
        ON i.folioviv = e.folioviv AND i.foliohog = e.foliohog 
        AND i.numren = e.numren AND i.anio = e.anio
    JOIN v_hogares_geo v 
        ON i.folioviv = v.folioviv AND i.foliohog = v.foliohog AND i.anio = v.anio
    WHERE e.edad BETWEEN 25 AND 65 AND i.ingreso_laboral > 0
    GROUP BY v.anio, v.entidad
)
SELECT 
    anio, entidad,
    w_prepa / NULLIF(w_secundaria, 0) as premio_prepa_sec,
    w_lic / NULLIF(w_prepa, 0) as premio_lic_prepa
FROM salarios_promedio;

-- 7. ESCOLARIDAD PADRES
DROP TABLE IF EXISTS padres_educacion;
CREATE TABLE padres_educacion AS
SELECT 
    folioviv, foliohog, anio,
    MAX(CASE WHEN parentesco = 101 THEN escolaridad_num ELSE 0 END) as edu_padre,
    MAX(CASE WHEN parentesco = 201 THEN escolaridad_num ELSE 0 END) as edu_madre
FROM v_educacion_normalizada
GROUP BY folioviv, foliohog, anio;

-- 8. TABLA FINAL UNIFICADA (Optimizada con LEFT JOIN)
CREATE TABLE tabla_analitica_final AS
SELECT 
    p.folioviv, p.foliohog, p.numren, p.anio,
    p.sexo, p.edad, p.asis_esc, p.escolaridad_num,
    p.etnia, 
    p.edo_conyug,
    i.ingreso_monetario, i.ingreso_laboral,
    h.ubica_geo, h.entidad, h.region, h.ambito, h.tot_integ, h.factor,
    pad.edu_padre, pad.edu_madre,
    r.premio_prepa_sec, r.premio_lic_prepa,
    -- Optimización: Usamos COALESCE del JOIN en lugar de subconsulta
    COALESCE(net.tiene_internet, 0) as tiene_internet

FROM v_educacion_normalizada p
JOIN v_hogares_geo h 
    ON p.folioviv = h.folioviv AND p.foliohog = h.foliohog AND p.anio = h.anio
LEFT JOIN v_ingresos_deflactados i 
    ON p.folioviv = i.folioviv AND p.foliohog = i.foliohog 
    AND p.numren = i.numren AND p.anio = i.anio
LEFT JOIN padres_educacion pad
    ON p.folioviv = pad.folioviv AND p.foliohog = pad.foliohog AND p.anio = pad.anio
LEFT JOIN ratios_estatales r
    ON h.entidad = r.entidad AND h.anio = r.anio
-- Aquí el JOIN mágico:
LEFT JOIN v_hogares_internet net
    ON p.folioviv = net.folioviv AND p.foliohog = net.foliohog AND p.anio = net.anio;

CREATE INDEX idx_final_anio_entidad ON tabla_analitica_final(anio, entidad);
CREATE INDEX idx_final_asis ON tabla_analitica_final(asis_esc);