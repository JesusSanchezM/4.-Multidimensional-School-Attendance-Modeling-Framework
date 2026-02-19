/*
=========================================================================
PROYECTO: UNIFICACIÓN ENIGH 2018 - 2024 (OPTIMIZADO PARA RENDIMIENTO)
OBJETIVO: Construcción de Tabla Maestra (Tablas intermedias + índices)
=========================================================================
*/

-- =========================================================================
-- 1. LIMPIEZA INICIAL
-- =========================================================================

DROP TABLE IF EXISTS tabla_analitica_final;
DROP TABLE IF EXISTS tmp_hogares_geo;
DROP TABLE IF EXISTS tmp_poblacion_base;
DROP TABLE IF EXISTS tmp_capital_humano;
DROP TABLE IF EXISTS tmp_ingresos_gastos;
DROP TABLE IF EXISTS tmp_ratios_salariales;


-- =========================================================================
-- 2. TABLA HOGARES + VIVIENDAS (materializada)
-- =========================================================================

CREATE TABLE tmp_hogares_geo AS
SELECT 
    h.folioviv, 
    h.foliohog, 
    h.anio, 
    h.factor, 
    h.ing_cor, 
    h.est_socio,
    
    v.upm, 
    v.est_dis, 
    v.ubica_geo,
    v.tam_loc,

    CASE 
        WHEN h.tot_integ > 10 THEN 10 
        ELSE h.tot_integ 
    END AS tot_integ,

    CASE 
        WHEN LENGTH(v.ubica_geo) = 4 THEN '0'||SUBSTR(v.ubica_geo, 1, 1) 
        ELSE SUBSTR(v.ubica_geo, 1, 2) 
    END AS entidad,

    CASE WHEN v.tam_loc = '4' THEN 'Rural' ELSE 'Urbano' END AS ambito,

    CASE 
        WHEN SUBSTR(printf('%02d', CAST(SUBSTR(v.ubica_geo,1,2) AS INTEGER)),1,2)
             IN ('02','08','05','19','26','28') THEN 'Norte'
        WHEN SUBSTR(printf('%02d', CAST(SUBSTR(v.ubica_geo,1,2) AS INTEGER)),1,2)
             IN ('03','25','18','10','32') THEN 'Norte-occidente'
        WHEN SUBSTR(printf('%02d', CAST(SUBSTR(v.ubica_geo,1,2) AS INTEGER)),1,2)
             IN ('14','01','06','16','24') THEN 'Centro-norte'
        WHEN SUBSTR(printf('%02d', CAST(SUBSTR(v.ubica_geo,1,2) AS INTEGER)),1,2)
             IN ('11','22','13','15','17','29','09','21') THEN 'Centro'
        WHEN SUBSTR(printf('%02d', CAST(SUBSTR(v.ubica_geo,1,2) AS INTEGER)),1,2)
             IN ('12','20','07','30','27','04','31','23') THEN 'Sur'
        ELSE 'Desconocido'
    END AS region,

    CASE 
        WHEN g.folioviv IS NOT NULL THEN 1
        ELSE 0
    END AS tiene_internet

FROM hogares h
JOIN viviendas v 
  ON h.folioviv = v.folioviv 
 AND h.anio = v.anio
LEFT JOIN (
    SELECT DISTINCT folioviv, foliohog
    FROM gastos_hogar
    WHERE clave IN ('R008','R010','R011')
) g
ON h.folioviv = g.folioviv AND h.foliohog = g.foliohog;

-- Índices para acelerar JOINs posteriores
CREATE INDEX idx_hogares_geo_folio ON tmp_hogares_geo(folioviv, foliohog, anio);


-- =========================================================================
-- 3. TABLA POBLACIÓN BASE (materializada)
-- =========================================================================

CREATE TABLE tmp_poblacion_base AS
SELECT 
    folioviv, foliohog, numren, anio, parentesco, sexo, edad, etnia, edo_conyug,

    CASE asis_esc 
        WHEN 1 THEN 'Asiste' 
        WHEN 2 THEN 'No asiste' 
        ELSE NULL 
    END AS asistencia_escolar,

    tipoesc, 
    otorg_b,

    COALESCE(NULLIF(TRIM(CAST(tiene_b AS TEXT)), ''), '0') * 1 AS tiene_beca,

    (COALESCE(hor_1,0)*60 + COALESCE(min_1,0) 
     + COALESCE(hor_3,0)*60 + COALESCE(min_3,0)) 
     AS tiempo_total_minutos,

    CASE 
        WHEN anio = 2018 AND (disc1 BETWEEN 1 AND 7) THEN 1 
        WHEN anio = 2018 AND disc1 = 8 THEN 0
        WHEN anio = 2024 AND (
             COALESCE(disc_ver,'') IN ('3','4') OR 
             COALESCE(disc_oir,'') IN ('3','4') OR
             COALESCE(disc_brazo,'') IN ('3','4') OR
             COALESCE(disc_camin,'') IN ('3','4') OR
             COALESCE(disc_apren,'') IN ('3','4') OR
             COALESCE(disc_vest,'') IN ('3','4') OR
             COALESCE(disc_habla,'') IN ('3','4') OR
             COALESCE(disc_acti,'') IN ('3','4')
        ) THEN 1
        ELSE 0 
    END AS tiene_discapacidad,

    CASE 
        WHEN nivelaprob IN (8,9) THEN 5 
        WHEN nivelaprob IN (5,6,7) AND gradoaprob >= 4 THEN 4 
        WHEN nivelaprob = 4 AND gradoaprob = 3 THEN 3 
        WHEN nivelaprob = 3 AND gradoaprob = 3 THEN 2 
        WHEN nivelaprob = 2 AND gradoaprob = 6 THEN 1 
        ELSE 0 
    END AS escolaridad_num

FROM poblacion;

CREATE INDEX idx_poblacion_base_folio ON tmp_poblacion_base(folioviv, foliohog, numren, anio);


-- =========================================================================
-- 4. CAPITAL HUMANO PADRES (materializado)
-- =========================================================================

CREATE TABLE tmp_capital_humano AS
SELECT 
    folioviv, foliohog, anio,
    MAX(CASE WHEN parentesco = 101 THEN escolaridad_num END) AS escolaridad_jefe,
    MAX(CASE WHEN parentesco = 201 THEN escolaridad_num END) AS escolaridad_conyuge
FROM tmp_poblacion_base
GROUP BY folioviv, foliohog, anio;

CREATE INDEX idx_capital_folio ON tmp_capital_humano(folioviv, foliohog, anio);


-- =========================================================================
-- 5. INGRESOS Y GASTOS (materializado con deflactor inline)
-- =========================================================================

CREATE TABLE tmp_ingresos_gastos AS
SELECT 
    i.folioviv, i.foliohog, i.numren, i.anio,

    SUM(
        CASE WHEN i.anio = 2024 THEN i.ing_tri * (100.0 / 133.543876114864) ELSE i.ing_tri END
    ) AS ingreso_monetario,

    SUM(
        CASE WHEN i.clave = 'P001' 
             THEN CASE WHEN i.anio = 2024 THEN i.ing_tri * (100.0 / 133.543876114864) ELSE i.ing_tri END
             ELSE 0 
        END
    ) AS ingreso_salarial,

    COALESCE(g.gasto_no_monetario,0) AS gasto_no_monetario

FROM ingresos i
LEFT JOIN (
    SELECT folioviv, foliohog, numren, anio, 
           SUM(CASE WHEN anio = 2024 THEN gas_nm_tri*(100.0/133.543876114864) ELSE gas_nm_tri END) AS gasto_no_monetario
    FROM gastos_persona
    GROUP BY folioviv, foliohog, numren, anio
) g
ON i.folioviv = g.folioviv AND i.foliohog = g.foliohog AND i.numren = g.numren AND i.anio = g.anio

GROUP BY i.folioviv, i.foliohog, i.numren, i.anio;

CREATE INDEX idx_ingresos_folio ON tmp_ingresos_gastos(folioviv, foliohog, numren, anio);


-- =========================================================================
-- 6. SALARIOS PROMEDIO POR GRUPO DE EDAD Y ESCOLARIDAD
-- =========================================================================

CREATE TABLE tmp_salarios_grupos AS
SELECT
    h.entidad,
    h.anio,
    
    -- Grupo 1: Secundaria terminada, no va a la escuela, edad 15-17
    AVG(CASE 
            WHEN p.asistencia_escolar='No asiste' 
                 AND p.escolaridad_num=2 
                 AND p.edad BETWEEN 15 AND 17 
            THEN i.ingreso_salarial 
       END
    ) AS salario_secundaria_15_17,

    -- Grupo 2: Preparatoria terminada, no va a la escuela, edad 18-24
    AVG(CASE 
            WHEN p.asistencia_escolar='No asiste' 
                 AND p.escolaridad_num=3 
                 AND p.edad BETWEEN 18 AND 24 
            THEN i.ingreso_salarial 
       END
    ) AS salario_prepa_18_24

FROM tmp_ingresos_gastos i
JOIN tmp_poblacion_base p 
  ON i.folioviv=p.folioviv 
 AND i.foliohog=p.foliohog 
 AND i.numren=p.numren 
 AND i.anio=p.anio
JOIN tmp_hogares_geo h 
  ON i.folioviv=h.folioviv 
 AND i.foliohog=h.foliohog 
 AND i.anio=h.anio

GROUP BY h.entidad, h.anio;

CREATE INDEX idx_salarios_grupos ON tmp_salarios_grupos(entidad, anio);



-- =========================================================================
-- 7. TABLA FINAL (JOIN de tablas materializadas)
-- =========================================================================

CREATE TABLE tabla_analitica_final AS
SELECT 
    p.folioviv, p.foliohog, p.numren, p.anio,
    p.sexo, p.edad, p.etnia, p.asistencia_escolar, 
    p.tiene_beca, p.tiene_discapacidad,
    p.tiempo_total_minutos, p.escolaridad_num,
    
    h.region, h.entidad, h.ambito, 
    h.tiene_internet, h.factor, h.est_socio, h.tot_integ, 
    
    ch.escolaridad_jefe, ch.escolaridad_conyuge,
    
    ing.ingreso_monetario, ing.ingreso_salarial, ing.gasto_no_monetario,
    (COALESCE(ing.ingreso_monetario,0)+COALESCE(ing.gasto_no_monetario,0)) AS ingreso_total_bruto,
    
    sg.salario_secundaria_15_17,
    sg.salario_prepa_18_24

FROM tmp_poblacion_base p
JOIN tmp_hogares_geo h 
  ON p.folioviv=h.folioviv AND p.foliohog=h.foliohog AND p.anio=h.anio
LEFT JOIN tmp_capital_humano ch 
  ON p.folioviv=ch.folioviv AND p.foliohog=ch.foliohog AND p.anio=ch.anio
LEFT JOIN tmp_ingresos_gastos ing 
  ON p.folioviv=ing.folioviv AND p.foliohog=ing.foliohog AND p.numren=ing.numren AND p.anio=ing.anio
LEFT JOIN tmp_salarios_grupos sg 
  ON h.entidad=sg.entidad AND h.anio=sg.anio;
