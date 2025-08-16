/****************************************************************** 
* QUERY CREADO POR: ALEXIS PINEDA 
* FECHA DE CREACIÓN: 08/08/2025 
* OBJETIVO: IDENTIFICAR CLIENTES QUE COMETAN UN POSIBLE FRAUDE 
* RIESGO: NPP (FIRST PAYMENT DEFAULT / SECOND PAYMENT DEFAULT / NEVER PAY) 
******************************************************************/ 
 --------------------------------------------CIERRES-------------------------------------------- 
DROP TABLE IF EXISTS proceso_bipa_vpr.ap_tb_fechas PURGE; 
CREATE TABLE proceso_bipa_vpr.ap_tb_fechas STORED AS PARQUET AS 
SELECT  
    fec_proceso AS fecha 
FROM 
    resultados_bipa_vpr.g_tb_fecha_mensual 
WHERE flag IN ('BPP','NPP')  
AND CAST(fec_proceso AS DATE) >= '2023-01-31' 
AND TO_DATE(fec_proceso) < NOW() 
order by fec_proceso ; 
 
COMPUTE STATS proceso_bipa_vpr.ap_tb_fechas; 
 --------------------------------------------PRODUCTOS------------------------------------------ 
DROP TABLE IF EXISTS PROCESO_BIPA_VPR.ap_TB_MAPEO_PRODUCTOS PURGE; 
CREATE TABLE IF NOT EXISTS 
    PROCESO_BIPA_VPR.ap_TB_MAPEO_PRODUCTOS  
        ( 
            PRODUCTO STRING,  
            PRODUCTOS STRING 
        )  
    STORED AS PARQUET; 
 
INSERT INTO 
    PROCESO_BIPA_VPR.ap_TB_MAPEO_PRODUCTOS (PRODUCTO, PRODUCTOS) 
VALUES 
    ('PRESTAMO HIPOTECARIO',                'PRESTAMO HIPOTECARIO'), 
    ('PRESTAMO HIPOTECARIO CREDIHOGAR',     'PRESTAMO HIPOTECARIO'), 
    ('PRESTAMO DE AUTO/MOTO',               'PRESTAMO AUTO'), 
    ('PRESTAMO AUTO',                       'PRESTAMO AUTO'), 
    ('PRESTAMO PERSONAL - VENTA DIGITAL',   'PRESTAMO PERSONAL'), 
    ('PRESTAMO PERSONAL',                   'PRESTAMO PERSONAL'), 
    ('BACK TO BACK LOANS PERSONAL',         'PRESTAMO GARANTIZADO'), 
    ('PRESTAMO GARANTIZADO',                'PRESTAMO GARANTIZADO'), 
    ('PRESTAMO FINANCOMER',                 'FINANCOMER'), 
    ('FINANCOMER',                          'FINANCOMER'); 
 
COMPUTE STATS PROCESO_BIPA_VPR.ap_TB_MAPEO_PRODUCTOS; 
 ----------------------------------------------CARTERA----------------------------------------------------- -- 3. CONSTRUCCIÓN DE CARTERA 
DROP TABLE IF EXISTS proceso_bipa_vpr.ap_tb_cartera PURGE; 
CREATE TABLE proceso_bipa_vpr.ap_tb_cartera STORED AS PARQUET AS 
WITH cartera_base AS ( -- Financomer 
SELECT 
    a.fec_proceso, 
    CAST(CAST(a.acreditado AS STRING) AS BIGINT) AS acreditado, 
    CAST(a.credito AS BIGINT) AS numcred, 
    a.saldoctaco AS saldo, 
    a.dpd, 
    CAST(a.fech_alta AS DATE) AS fech_alta, 
    TO_TIMESTAMP(CAST(a.fec_pri_pago AS STRING),'yyyyMMdd') AS fec_pri_pago, 
    CAST(ADDDATE(TO_TIMESTAMP(CAST(a.fec_pri_pago AS STRING),'yyyyMMdd'),30) AS 
DATE) AS fec_sig_pago, 
    'FINANCOMER' AS producto, 
    a.importe 
FROM resultados_bipa_vpr.fi_k_tb_morosidad a 
INNER JOIN proceso_bipa_vpr.ap_tb_fechas b ON TO_DATE(a.fec_proceso) = TO_DATE(b.fecha) 
WHERE CAST(a.fech_alta AS DATE) >= '2023-01-01' 
 
UNION ALL 
 -- Tarjeta de Crédito 
SELECT 
    a.fecha_proceso, 
    CAST((CASE  
        WHEN CAST(cliente_hogan AS STRING) LIKE '%á%' THEN REPLACE(CAST(cliente_hogan 
AS STRING),'á','') ELSE CAST(cliente_hogan AS STRING) 
    END) AS BIGINT) AS ACREDITADO, 
    CAST(a.numcuenta AS BIGINT) AS numcred, 
    a.balanceactual AS saldo, 
    a.diasmorosidad AS dpd, 
    CAST(a.fecapertura AS DATE) AS fech_alta, 
    a.fec_pri_pag, 
    CAST(ADDDATE(a.fec_pri_pag,  30) AS DATE) AS fec_sig_pago, 
    'TARJETA DE CREDITO', 
    a.crlim 
FROM resultados_bipa_vpr.md_tb_cons_tdc a 
INNER JOIN proceso_bipa_vpr.ap_tb_fechas b ON TO_DATE(a.fecha_proceso) = 
TO_DATE(b.fecha) 
WHERE CAST(a.fecapertura AS DATE) >= '2023-01-01' 
 
UNION ALL 
 
-- Resto de productos 
SELECT 
    a.fec_proceso, 
    CAST((CASE  
        WHEN CAST(a.acreditado AS STRING) LIKE '%á%' THEN REPLACE(CAST(a.acreditado AS 
STRING),'á','') ELSE CAST(a.acreditado AS STRING) 
    END) AS BIGINT) AS ACREDITADO, 
    CAST(a.numcred AS BIGINT) AS numcred, 
    a.saldocapital, 
    a.dpd, 
    CAST(a.fech_alta AS DATE) AS fech_alta, 
    a.fec_pri_pago, 
    CAST(ADDDATE(a.fec_pri_pago, 30) AS DATE) AS fec_sig_pago, 
    m.productos, 
    a.importe 
FROM resultados_bipa_vpr.k_rpt_pfs a 
INNER JOIN proceso_bipa_vpr.ap_tb_fechas b ON TO_DATE(a.fec_proceso) = TO_DATE(b.fecha) 
LEFT JOIN proceso_bipa_vpr.ap_tb_mapeo_productos m ON UPPER(TRIM(a.producto)) = 
UPPER(TRIM(m.producto)) 
WHERE  CAST(a.fech_alta AS DATE) >= '2023-01-01' 
), 
tdc_fec_pri AS ( 
SELECT 
    credito_NEW, 
    CAST( 
        (CASE  
            WHEN PRODUCTO = 'TARJETA DE CREDITO' AND CAST(acreditado_NEW AS STRING) 
LIKE '%á%' THEN REPLACE(CAST(acreditado_NEW AS STRING),'á','')  
            --WHEN acreditado <> Acreditado_OLD THEN Acreditado_OLD 
        ELSE CAST(acreditado_NEW AS STRING) 
        END) 
    AS BIGINT) AS acreditado, 
CAST(MIN(fecha_proximo_pago) AS DATE) AS fec_primer_pago 
FROM resultados_bipa_vpr.tb_fact_morosidad 
WHERE YEAR(fecha_proceso) >= YEAR(NOW()) - 2 AND fecha_proximo_pago IS NOT NULL 
GROUP BY credito_NEW,acreditado 
) 
SELECT 
c.*, 
CASE  
WHEN c.producto = 'TARJETA DE CREDITO' THEN t.fec_primer_pago -- WHEN CAST(c.acreditado AS BIGINT) = CAST(t.acreditado_NEW AS BIGINT) AND 
c.fec_pri_pago <> t.fec_primer_pago THEN t.fec_primer_pago 
ELSE CAST(c.fec_pri_pago AS DATE) 
END AS primer_pago 
FROM cartera_base c 
LEFT JOIN tdc_fec_pri t ON CAST(c.numcred AS BIGINT) = CAST(t.credito_NEW AS BIGINT) 
AND CAST(c.acreditado AS BIGINT) = CAST(t.acreditado AS BIGINT); 
COMPUTE STATS proceso_bipa_vpr.ap_tb_cartera; -------------------------------------------------------------------------------------------------------------------------------------- -- 4. FPD PARTE 1 (Evaluación de mes de primer pago) 
DROP TABLE IF EXISTS proceso_bipa_vpr.ap_tb_FPD_pre PURGE; 
CREATE TABLE proceso_bipa_vpr.ap_tb_FPD_pre STORED AS PARQUET AS 
WITH fechas AS ( 
SELECT 
CAST(fecha AS DATE) AS fecha 
FROM proceso_bipa_vpr.ap_tb_fechas 
WHERE CAST(fecha AS DATE) >= '2023-01-31' AND TO_DATE(fecha) < NOW() 
), 
FPD_EVAL AS ( 
SELECT 
    f.fecha AS fec_cierre, 
    c.*, 
    CASE  
        WHEN EXTRACT(YEAR  FROM primer_pago) = EXTRACT(YEAR  FROM f.fecha)  
         AND EXTRACT(MONTH FROM primer_pago) = EXTRACT(MONTH FROM f.fecha) THEN 1 
    ELSE 0 
END AS mes_evaluar, 
    CASE 
        WHEN primer_pago IS NULL THEN 'Error en Data' 
        WHEN dpd > 0 
        AND EXTRACT(YEAR  FROM primer_pago) = EXTRACT(YEAR  FROM f.fecha)  
        AND EXTRACT(MONTH FROM primer_pago) = EXTRACT(MONTH FROM f.fecha) THEN 'Si' 
    ELSE 'No' 
END AS fpd 
FROM proceso_bipa_vpr.ap_tb_cartera c 
INNER JOIN fechas f ON TO_DATE(c.fec_proceso) = f.fecha 
) 
SELECT 
    * 
FROM FPD_EVAL 
WHERE mes_evaluar = 1 
ORDER BY numcred, fec_cierre; 
 
COMPUTE STATS proceso_bipa_vpr.ap_tb_FPD_pre; ----------------------------------------------------------------------------------------------------------------------------------------- -- 5. FPD PARTE 2 (Confirmación de mora a 30 días) 
DROP TABLE IF EXISTS proceso_bipa_vpr.ap_tb_FPD PURGE; 
CREATE TABLE proceso_bipa_vpr.ap_tb_FPD STORED AS PARQUET AS 
WITH FACTOS AS ( 
SELECT 
    ROW_NUMBER() over (Partition by credito_new, 
    CAST((CASE  
        WHEN CAST(a.acreditado AS STRING) LIKE '%á%' THEN REPLACE(CAST(a.acreditado AS 
STRING),'á','') ELSE CAST(a.acreditado AS STRING) 
    END) AS BIGINT)  
    ,B.primer_pago order by A.fecha_proceso,B.primer_pago ASC) as 'ORDEN', 
    A.fecha_proceso, 
    A.credito_new, 
    CAST((CASE  
        WHEN CAST(a.acreditado AS STRING) LIKE '%á%' THEN REPLACE(CAST(a.acreditado AS 
STRING),'á','') ELSE CAST(a.acreditado AS STRING) 
    END) AS BIGINT) AS acreditado, 
    A.dias_morosidad, 
    B.primer_pago as primer_pago, 
    DATEDIFF(A.fecha_proceso,B.primer_pago) AS DIF 
FROM resultados_bipa_vpr.tb_fact_morosidad A 
INNER JOIN proceso_bipa_vpr.ap_tb_FPD_pre B ON CAST(A.credito_new AS BIGINT) = 
CAST(B.numcred AS BIGINT) 
WHERE A.fecha_proceso BETWEEN B.primer_pago AND ADDDATE(B.primer_pago,30) 
), 
FACT AS ( 
SELECT  
credito_new,  
CAST((CASE  
        WHEN CAST(acreditado AS STRING) LIKE '%á%' THEN REPLACE(CAST(acreditado AS 
STRING),'á','') ELSE CAST(acreditado AS STRING) 
    END) AS BIGINT) AS acreditado,  
MIN(fecha_proceso) AS fecha_proceso, MIN(dias_morosidad) AS dias_morosidad, 
MIN(primer_pago) AS primer_pago, MIN(CASE WHEN dias_morosidad > 0 THEN 1 ELSE 0 END) 
AS fpd_30_dias 
FROM FACTOS 
WHERE dias_morosidad >0  
GROUP BY 1,2), 
ap_tb_FPD AS ( 
SELECT  
    ROW_NUMBER() over (Partition by numcred,A.acreditado order by fec_cierre,B.primer_pago 
ASC) as 'ORDEN',  
    A.fec_cierre, 
    A.acreditado, 
    A.numcred, 
    A.producto, 
    A.importe, 
    A.saldo, 
    A.dpd AS DPD_antes, 
    CASE WHEN A.fpd = 'No' AND COALESCE(B.fpd_30_dias,0) = 1 THEN b.dias_morosidad 
ELSE A.dpd END AS fpd_dpd, 
    A.fech_alta, 
    A.primer_pago, 
    A.fec_sig_pago, 
    A.fpd AS fpd_antes, 
    COALESCE(B.fpd_30_dias,0) AS fpd_30_dias, 
    CASE WHEN A.fpd = 'No' AND COALESCE(B.fpd_30_dias,0) = 1 THEN 'Si' ELSE A.fpd END AS 
fpd, 
    EXTRACT(YEAR  FROM A.primer_pago) AS mes_reporte_anio, 
    EXTRACT(MONTH FROM A.primer_pago) AS mes_reporte_mes 
FROM proceso_bipa_vpr.ap_tb_FPD_pre A 
LEFT JOIN FACT B ON CAST(A.numcred AS BIGINT) = CAST(B.credito_new AS BIGINT) AND 
CAST(A.acreditado AS BIGINT) = CAST(B.acreditado AS BIGINT) -- AND B.ORDEN = 1 
WHERE A.mes_evaluar = 1) 
SELECT ORDEN,fec_cierre, acreditado, numcred, producto, importe, saldo, fpd_dpd, fech_alta, 
primer_pago, fec_sig_pago, fpd_30_dias, fpd ,mes_reporte_anio, mes_reporte_mes  
FROM ap_tb_FPD  
WHERE ORDEN = 1 
; 
 
COMPUTE STATS proceso_bipa_vpr.ap_tb_FPD; 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------- --SPD (Second Payment Default) - Atribución al mes del segundo pago 
/****************************************************************** 
* SPD MODULE - Atribución al MES DEL SEGUNDO PAGO + Trazabilidad 
* Autor: Alexis Pineda - 09/08/2025 
* Dependencias previas: 
* - proceso_bipa_vpr.ap_tb_cartera (con primer_pago y fec_sig_pago) 
* - proceso_bipa_vpr.ap_tb_FPD (con fpd_final/fpd) 
******************************************************************/ -- 1) Candidatos a SPD (excluye quienes ya son FPD en el mes del primer pago) 
DROP TABLE IF EXISTS proceso_bipa_vpr.ap_tb_SPD_candidatos PURGE; 
CREATE TABLE proceso_bipa_vpr.ap_tb_SPD_candidatos STORED AS PARQUET AS 
SELECT 
    c.fec_proceso, 
    c.acreditado, 
    c.numcred, 
    c.producto, 
    c.importe, 
    c.saldo, 
    c.dpd, 
    c.fech_alta, 
    c.fec_pri_pago, 
    c.primer_pago, 
    c.fec_sig_pago 
    -- segundo pago programado 
FROM proceso_bipa_vpr.ap_tb_cartera c 
LEFT JOIN proceso_bipa_vpr.ap_tb_FPD f ON CAST(c.numcred AS BIGINT) = CAST(f.numcred 
AS BIGINT) 
                                            AND CAST(c.acreditado AS BIGINT) = CAST(f.acreditado AS BIGINT) 
                                            AND TO_DATE(c.fec_proceso) = TO_DATE(f.fec_cierre) 
WHERE 
    -- Elegimos solo las filas del mes del PRIMER PAGO 
    EXTRACT(YEAR  FROM c.primer_pago) = EXTRACT(YEAR  FROM c.fec_proceso) 
AND EXTRACT(MONTH FROM c.primer_pago) = EXTRACT(MONTH FROM c.fec_proceso) 
; 
 
COMPUTE STATS proceso_bipa_vpr.ap_tb_SPD_candidatos; ----------------------------------------------------------------------------------------- -- 2) Hechos de mora dentro de la ventana de 30 días a partir del SEGUNDO pago -- Capturamos la PRIMERA fecha con mora > 0 y la marcamos para trazabilidad. 
DROP TABLE IF EXISTS proceso_bipa_vpr.ap_tb_SPD_fact PURGE; 
CREATE TABLE proceso_bipa_vpr.ap_tb_SPD_fact STORED AS PARQUET AS 
WITH hechos AS ( 
SELECT 
    A.credito_new, 
    CAST(REPLACE(CAST(A.acreditado AS STRING), 'á', '') AS BIGINT) AS acreditado, 
    B.fec_sig_pago, 
    A.fecha_proceso, 
    A.dias_morosidad, 
    DATEDIFF(A.fecha_proceso,B.fec_sig_pago) AS dif_dias 
FROM resultados_bipa_vpr.tb_fact_morosidad A 
INNER JOIN proceso_bipa_vpr.ap_tb_SPD_candidatos B ON CAST(A.credito_new AS BIGINT) = 
CAST(B.numcred AS BIGINT) 
        AND CAST(REPLACE(CAST(A.acreditado AS STRING), 'á', '') AS BIGINT) = 
CAST(B.acreditado AS BIGINT) 
WHERE A.fecha_proceso BETWEEN B.fec_sig_pago AND ADDDATE(B.fec_sig_pago,30) 
) 
SELECT 
    credito_new, 
    acreditado, 
    MIN(CASE WHEN dias_morosidad > 0 THEN 1 ELSE 0 END) AS spd_30_dias, 
    -- existe mora en ventana 
    MIN(CASE WHEN dias_morosidad > 0 THEN fecha_proceso END) AS fecha_primer_dpd_spd, 
    MIN(CASE WHEN dias_morosidad > 0 THEN dias_morosidad END) AS 
dpd_en_primer_dpd_spd, 
    MIN(fec_sig_pago) AS fec_segundo_pago 
FROM hechos 
WHERE dias_morosidad > 0 
GROUP BY 1, 2; 
 
COMPUTE STATS proceso_bipa_vpr.ap_tb_SPD_fact; 
 -- 3) Resultado SPD con atribución al mes del SEGUNDO pago 
DROP TABLE IF EXISTS proceso_bipa_vpr.ap_tb_SPD PURGE; 
CREATE TABLE proceso_bipa_vpr.ap_tb_SPD STORED AS PARQUET AS 
SELECT 
    B.fec_proceso, 
    B.acreditado, 
    B.numcred, 
    B.producto, 
    B.importe, 
    B.saldo, 
    B.dpd, 
    B.fech_alta, 
    B.primer_pago, 
    B.fec_sig_pago, 
    COALESCE(F.spd_30_dias, 0) AS spd_30_dias, 
    CASE 
        WHEN COALESCE(F.spd_30_dias,0) = 1 THEN 'Si' 
        ELSE 'No' 
    END AS spd, 
    F.fecha_primer_dpd_spd, 
    F.dpd_en_primer_dpd_spd, 
    EXTRACT(YEAR  FROM B.fec_sig_pago) AS mes_reporte_anio, 
EXTRACT(MONTH FROM B.fec_sig_pago) AS mes_reporte_mes 
FROM proceso_bipa_vpr.ap_tb_SPD_candidatos B 
LEFT JOIN proceso_bipa_vpr.ap_tb_SPD_fact F ON CAST(B.numcred AS BIGINT) = 
CAST(F.credito_new AS BIGINT) AND CAST(B.acreditado AS BIGINT) = CAST(F.acreditado AS 
BIGINT) 
ORDER BY B.numcred, B.fec_proceso; 
COMPUTE STATS proceso_bipa_vpr.ap_tb_SPD; -------------------------------------------------UNIFICACION DE FPD Y SPD----------------------------------------------------------------------------------- 
DROP TABLE IF EXISTS proceso_bipa_vpr.ap_tb_FPD_SPD PURGE; 
CREATE TABLE proceso_bipa_vpr.ap_tb_FPD_SPD STORED AS PARQUET AS 
SELECT  
orden, fec_cierre, A.acreditado, A.numcred, A.producto, A.importe, A.saldo, A.fpd_dpd, 
A.fech_alta, A.primer_pago, A.fec_sig_pago, A.fpd, B.spd 
FROM proceso_bipa_vpr.ap_tb_FPD A 
LEFT JOIN proceso_bipa_vpr.ap_tb_SPD B ON A.numcred = B.numcred AND A.acreditado = 
B.acreditado AND UPPER(TRIM(A.producto)) = UPPER(TRIM(B.producto)) ; 
COMPUTE STATS proceso_bipa_vpr.ap_tb_FPD_SPD; ------------------------------------------------------------------------------------------------------------------------------------------------------------- 
/****************************************************************** 
* NEVER PAY (NP) MODULE - Atribución al MES DEL PRIMER PAGO + Trazabilidad 
* Autor: Alexis Pineda - 09/08/2025 
* Objetivo: Identificar clientes que NUNCA han realizado pago desde el PRIMER PAGO, 
* unificando y corrigiendo la lógica previa (tolerancia de 15 días). 
* Dependencias previas (coherentes con FPD/SPD V2 y tu script FPD_4): 
* - proceso_bipa_vpr.ap_tb_cartera (contiene primer_pago, fec_sig_pago, acreditado 
normalizado) 
* - proceso_bipa_vpr.ap_tb_FPD (atribución mes primer pago) 
* - proceso_bipa_vpr.ap_tb_SPD (atribución mes segundo pago) 
******************************************************************/ -- 1) Último estado por crédito para medir situación actual (DPD_POST) y rango desde primer 
pago 
DROP TABLE IF EXISTS proceso_bipa_vpr.ap_tb_NP_latest PURGE; 
CREATE TABLE proceso_bipa_vpr.ap_tb_NP_latest STORED AS PARQUET AS 
WITH latest_fact AS ( 
SELECT 
    A.credito_new, 
    CAST(REPLACE(CAST(A.acreditado AS STRING), 'á', '') AS BIGINT) AS acreditado, 
    CASE 
        WHEN UPPER(TRIM(A.producto)) IN ('PRESTAMO DE AUTO/MOTO', 'PRESTAMO AUTO') 
THEN 'PRESTAMO AUTO' 
        ELSE UPPER(TRIM(A.producto)) 
    END AS producto_norm, 
    MAX(A.fecha_proceso) AS max_fecha_proceso 
FROM resultados_bipa_vpr.tb_fact_morosidad A 
WHERE CAST(A.fecha_alta AS DATE) >= '2023-01-01' 
GROUP BY 1, 2, 3 
), 
ultimo AS ( 
SELECT 
    F.credito_new, 
    F.acreditado, 
    F.producto_norm, 
    F.max_fecha_proceso, 
    A.dias_morosidad AS dpd_post, 
    UPPER(TRIM(A.categoria_estado_credito)) AS categoria_estado_credito 
FROM latest_fact F 
INNER JOIN resultados_bipa_vpr.tb_fact_morosidad A ON A.credito_new = F.credito_new 
        AND CAST(REPLACE(CAST(A.acreditado AS STRING), 'á', '') AS BIGINT) = F.acreditado 
        AND A.fecha_proceso = F.max_fecha_proceso 
) 
SELECT 
    C.fec_proceso, 
    C.acreditado, 
    C.numcred, 
    UPPER(TRIM(C.producto)) AS producto, 
    C.importe, 
    C.saldo, 
    C.dpd, 
    C.fech_alta, 
    C.fec_pri_pago, 
    C.primer_pago, 
    C.fec_sig_pago, 
    U.max_fecha_proceso, 
    U.dpd_post, 
    U.categoria_estado_credito 
FROM proceso_bipa_vpr.ap_tb_cartera C 
INNER JOIN ultimo U ON CAST(C.numcred AS BIGINT) = CAST(U.credito_new AS BIGINT) AND 
CAST(C.acreditado AS BIGINT) = CAST(U.acreditado AS BIGINT); 
 
COMPUTE STATS proceso_bipa_vpr.ap_tb_NP_latest; 
 ---------------------------------------------------------------------------------------------------------------- -- 2) ¿Alguna evidencia de pago desde el PRIMER PAGO? -- Señal negativa para NP: existe algún día con DPD = 0 entre PRIMER_PAGO y 
MAX_FECHA_PROCESO 
DROP TABLE IF EXISTS proceso_bipa_vpr.ap_tb_NP_any_payment PURGE; 
 
CREATE TABLE proceso_bipa_vpr.ap_tb_NP_any_payment STORED AS PARQUET AS 
SELECT 
    L.numcred, 
    L.acreditado, 
    MIN(CASE WHEN F.dias_morosidad = 0 THEN 1 ELSE 0 END) AS hubo_pago_en_ventana 
FROM proceso_bipa_vpr.ap_tb_NP_latest L 
LEFT JOIN resultados_bipa_vpr.tb_fact_morosidad F ON 
    CAST(F.credito_new AS BIGINT) = CAST(L.numcred AS BIGINT) 
    AND CAST(REPLACE(CAST(F.acreditado AS STRING), 'á', '') AS BIGINT) = CAST(L.acreditado 
AS BIGINT) 
    AND F.fecha_proceso BETWEEN L.primer_pago AND L.max_fecha_proceso 
GROUP BY L.numcred, L.acreditado; 
 
COMPUTE STATS proceso_bipa_vpr.ap_tb_NP_any_payment; 
 ----------------------------------------------------------------------------------------------------------------------------- -- 3) Evaluación de NEVER PAY con tolerancia de 15 días (consistente con tu lógica previa) -- Regla base: si NO hubo pago en ventana y DPD_POST ~ días desde PRIMER_PAGO, marcamos 
NP. 
DROP TABLE IF EXISTS proceso_bipa_vpr.ap_tb_NP PURGE; 
CREATE TABLE proceso_bipa_vpr.ap_tb_NP STORED AS PARQUET AS 
WITH base AS ( 
SELECT DISTINCT  
    L.fec_proceso, 
    L.acreditado, 
    L.numcred, 
    L.producto, 
    L.importe, 
    L.saldo, 
    L.dpd, 
    L.fech_alta, 
    L.fec_pri_pago, 
    L.primer_pago, 
    L.fec_sig_pago, 
    L.max_fecha_proceso, 
    L.dpd_post, 
    L.categoria_estado_credito, 
    P.hubo_pago_en_ventana, 
    DATEDIFF(CAST(L.max_fecha_proceso AS DATE), 
    CAST(L.primer_pago AS DATE)) AS dias_desde_primer_pago 
FROM proceso_bipa_vpr.ap_tb_NP_latest L 
LEFT JOIN proceso_bipa_vpr.ap_tb_NP_any_payment P ON L.numcred = P.numcred AND 
L.acreditado = P.acreditado), 
eval AS ( 
SELECT 
    b.*, 
    CASE 
        WHEN COALESCE(b.hubo_pago_en_ventana, 0) = 1 THEN 0 
        -- hubo al menos un día con DPD=0 
        WHEN b.dpd_post IS NULL THEN 0 
        WHEN (b.dias_desde_primer_pago <= b.dpd_post) THEN 1 
        WHEN ABS(b.dias_desde_primer_pago - b.dpd_post) <= 15 THEN 1 
        -- tolerancia ±15 días 
        ELSE 0 
    END AS flag_never_pay 
FROM base b), 
eval_f as ( 
SELECT 
    ROW_NUMBER() OVER ( 
    PARTITION BY e.numcred, e.acreditado  
    ORDER BY e.fec_proceso DESC 
    ) AS orden, 
    e.fec_proceso, 
    e.acreditado, 
    e.numcred, 
    e.producto, 
    e.importe, 
    e.saldo, 
    e.dpd, 
    e.fech_alta, 
    e.fec_pri_pago, 
    e.primer_pago, 
    e.fec_sig_pago, 
    e.max_fecha_proceso, 
    e.dpd_post, 
    e.categoria_estado_credito, 
    e.hubo_pago_en_ventana, 
    e.dias_desde_primer_pago, 
    CASE 
        WHEN e.flag_never_pay = 1 THEN 'Si' 
        ELSE 'No' 
    END AS never_pay, 
    -- Clasificación ejecutiva similar a tu versión 
    CASE 
        WHEN e.flag_never_pay = 1 AND COALESCE(e.dpd_post, 0) < 1 THEN 'Nunca pago - 
Recuperado' 
        WHEN e.flag_never_pay = 1 THEN 'Nunca pago' 
        WHEN e.dpd > 0 AND COALESCE(e.dpd_post, 0) = 0 THEN 'Recuperado' 
        WHEN e.dpd > 0 AND COALESCE(e.dpd_post, 0) < e.dpd THEN 'Pagó después del FPD' 
        WHEN e.dpd > 0 THEN 'En riesgo' 
        WHEN e.dpd = 0 AND COALESCE(e.dpd_post, 0) > 0 THEN 'En Riesgo - No marcó FPD' 
        WHEN e.dpd = 0 AND COALESCE(e.dpd_post, 0) = 0 THEN 'Estable' 
        ELSE 'Revisar' 
    END AS clas_clave, 
    EXTRACT(YEAR  FROM e.primer_pago) AS mes_reporte_anio, EXTRACT(MONTH FROM 
e.primer_pago) AS mes_reporte_mes 
FROM eval e) 
SELECT 
    orden, 
    fec_proceso, 
    acreditado, 
    numcred, 
    producto, 
    importe, 
    saldo, 
    dpd, 
    fech_alta, 
    fec_pri_pago, 
    primer_pago, 
    fec_sig_pago, 
    max_fecha_proceso, 
    dpd_post, 
    categoria_estado_credito, 
    hubo_pago_en_ventana, 
    dias_desde_primer_pago, 
    never_pay, 
    clas_clave, 
    mes_reporte_anio, 
    mes_reporte_mes 
FROM eval_f  
WHERE ORDEN = 1; 
 
COMPUTE STATS proceso_bipa_vpr.ap_tb_NP; 
 --------------------------------------------------------------------------------------------------------------- -- 4) Unificación FPD + SPD + NP (vista final para consumo analítico) 
DROP TABLE IF EXISTS proceso_bipa_vpr.ap_tb_FPD_cONSUMO PURGE; 
CREATE TABLE proceso_bipa_vpr.ap_tb_FPD_cONSUMO STORED AS PARQUET AS 
SELECT DISTINCT  
    f.orden, 
    f.fec_cierre, 
    f.acreditado, 
    f.numcred, 
    f.producto, 
    f.importe, 
    f.saldo, 
    f.fpd_dpd, 
    f.fech_alta, 
    f.primer_pago, 
    f.fec_sig_pago, 
    f.fpd, 
    s.spd, 
    n.never_pay, 
    n.clas_clave, 
    COALESCE(n.dpd_post,f.fpd_dpd) AS dpd_post, 
    -- prioridad a estado más reciente 
    COALESCE(n.categoria_estado_credito, '') AS categoria_estado_credito, 
    f.mes_reporte_anio, 
    f.mes_reporte_mes 
FROM proceso_bipa_vpr.ap_tb_FPD f 
LEFT JOIN proceso_bipa_vpr.ap_tb_SPD s ON f.numcred = s.numcred 
                                            AND f.acreditado = s.acreditado 
                                            AND UPPER(TRIM(f.producto)) = UPPER(TRIM(s.producto)) 
LEFT JOIN proceso_bipa_vpr.ap_tb_NP n ON f.numcred = n.numcred 
                                            AND f.acreditado = n.acreditado 
                                            AND UPPER(TRIM(f.producto)) = UPPER(TRIM(n.producto)); 
 
COMPUTE STATS proceso_bipa_vpr.ap_tb_FPD_cONSUMO; -------------------------------------------------------------------------------------------------------------------------------------- 
--CONTROL DE CALIDAD (CONTEO DE DUPLICADOS) 
with validacion as 
(SELECT 
numcred, 
count(*) as items 
FROM proceso_bipa_vpr.ap_tb_FPD_cONSUMO 
group by numcred) 
select * from validacion  
where items > 1  
order by items desc; 
; 
SELECT  
orden, fec_cierre, acreditado, numcred, producto, importe, saldo, fpd_dpd, fech_alta, 
primer_pago, fec_sig_pago, fpd, spd, never_pay, clas_clave, dpd_post, 
categoria_estado_credito, mes_reporte_anio, mes_reporte_mes 
FROM proceso_bipa_vpr.ap_tb_FPD_cONSUMO 
; 
