# -- coding: utf-8 --
import sys
reload(sys)
sys.setdefaultencoding('utf8')
# fuentes:
# db_cs_altas.otc_t_altas_bi
# db_cs_altas.otc_t_transfer_in_bi
# db_cmd.otc_t_dm_cur_t2 
# db_trafica.otc_t_cur_voz_trafica

# destino:
# db_reportes.trafico_captacion
# """

def q_generar_universo_altas(FECHA_INI,FECHA_FIN):
    qry = """
    SELECT telefono,
        CASE 
            WHEN portabilidad = 'SI' THEN 'Portabilidad' 
            WHEN portabilidad = 'INTRA' THEN 'Portabilidad' 
            ELSE 'Alta' 
        END AS tipo_movimiento,
        fecha_alta AS fecha_movimiento,
        CASE      
            WHEN SEGMENTO_FIN = 'TITANIUM' THEN 'B2C'    
            WHEN SEGMENTO_FIN = 'SIN SEGMENTO' THEN 'B2C'    
            WHEN SEGMENTO_FIN = 'MASIVOS' THEN 'B2C'    
            WHEN SEGMENTO_FIN = 'INDIVIDUAL' THEN 'B2C'    
            WHEN SEGMENTO_FIN = 'GOLD' THEN 'B2C'    
            WHEN SEGMENTO_FIN = 'CARIBU' THEN 'B2C'    
            WHEN SEGMENTO_FIN = 'ALTO VALOR' THEN 'B2C'    
            WHEN SEGMENTO_FIN = 'SILVER' THEN 'B2C'    
            WHEN SEGMENTO_FIN = 'PYMES' THEN 'B2B'    
            WHEN SEGMENTO_FIN = 'OTROS' THEN 'B2B'    
            WHEN SEGMENTO_FIN = 'NEGOCIOS' THEN 'B2B'    
            WHEN SEGMENTO_FIN = 'GRANDES CUENTAS' THEN 'B2B'    
            WHEN SEGMENTO_FIN = 'EMPRESAS' THEN 'B2B'    
            WHEN SEGMENTO_FIN = 'TELEFONIA PUBLICA' THEN 'B2B'    
            WHEN SEGMENTO_FIN = '' THEN 'B2C'
            ELSE SEGMENTO_FIN    
        END AS segmento
    FROM db_cs_altas.otc_t_altas_bi
    WHERE p_fecha_proceso >= {FECHA_INI}  
        AND p_fecha_proceso <= {FECHA_FIN} 
        AND LINEA_NEGOCIO <> 'PREPAGO'
        AND SUB_SEGMENTO NOT IN ('OTECEL', 'TELEFONÃA PÃšBLICA', 'TELEFONIA PUBLICA', 'CANALES EQUIPOS','MASIVO')
        AND CATEGORIA_PLAN = 'VOZ' 
        AND PLAN_CODIGO <> 'S2' 
        AND SEGMENTO_FIN IN ('EMPRESAS', 'GRANDES CUENTAS', 'NEGOCIOS', 'PYMES')
    """.format(FECHA_INI=FECHA_INI, FECHA_FIN=FECHA_FIN)
    return qry

def q_generar_universo_transferencias(FECHA_INI,FECHA_FIN):
    qry = """
    SELECT telefono,
        "Transferencia" AS tipo_movimiento,
        fecha_transferencia AS fecha_movimiento,
        CASE      
            WHEN segmento_actual = 'INDIVIDUAL' THEN 'B2C'    
            WHEN segmento_actual = 'GGCC' THEN 'B2B'    
            WHEN segmento_actual = 'NEGOCIOS' THEN 'B2B' 
            ELSE segmento_actual    
        END AS segmento
    FROM db_cs_altas.otc_t_transfer_in_bi
    WHERE p_fecha_proceso >= {FECHA_INI}  
        AND p_fecha_proceso <= {FECHA_FIN} 
        AND TIPO_DOC_CLIENTE IN ('RUC','RUC Personal','RUC/RUC Personal')
        AND LINEA_NEGOCIO <> 'PREPAGO' 
        AND CATEGORIA_ACTUAL = 'VOZ'
        AND SUB_SEGMENTO_ACTUAL NOT IN ('OTECEL','TELEFONÃA PÃšBLICA','TELEFONIA PUBLICA','CANALES EQUIPOS','MASIVO','MASIVO MIGRADO','NUEVOS INDIVIDUALES','CICLO ANTICIPADO NUEVOS INDIVIDUALES') 
        AND CODIGO_PLAN_ANTERIOR <> 'S2'
    """.format(FECHA_INI=FECHA_INI, FECHA_FIN=FECHA_FIN)
    return qry

def q_generar_universo_trafico_captacion(vSChema):
    qry = """
    SELECT tipo_movimiento,
        telefono,
        fecha_movimiento,
        segmento,
        date_format(fecha_movimiento, 'yyyyMMdd') fecha_alta,
        date_format(date_add(fecha_movimiento, 6),'yyyyMMdd') fecha_alta_7,
        date_format(date_add(fecha_movimiento, 14),'yyyyMMdd') fecha_alta_15,
        date_format(date_add(fecha_movimiento, 29),'yyyyMMdd') fecha_alta_30
    FROM {vSChema}.universo_trafico_captacion
    """.format(vSChema=vSChema)
    return qry

def q_generar_ventanas_moviles(FECHA_EJECUCION):
    qry = """
    SELECT tipo_movimiento,
        telefono,
        fecha_movimiento,
        segmento,
        fecha_alta,
        CASE WHEN fecha_alta_7 > {FECHA_EJECUCION} THEN {FECHA_EJECUCION} ELSE fecha_alta_7 END AS fecha_alta_7,
        CASE WHEN fecha_alta_15 > {FECHA_EJECUCION} THEN {FECHA_EJECUCION} ELSE fecha_alta_15 END AS fecha_alta_15,
        CASE WHEN fecha_alta_30 > {FECHA_EJECUCION} THEN {FECHA_EJECUCION} ELSE fecha_alta_30 END AS fecha_alta_30
    FROM universo_trafico_captacion
    """.format(FECHA_EJECUCION=FECHA_EJECUCION)
    return qry

def q_generar_reporte_trafico_datos(FECHA_INICIO, FECHA_FIN):
    qry = """
    SELECT dat.numeroorigen as telefono,
        univ.tipo_movimiento,
        univ.fecha_movimiento,
        univ.segmento,
        univ.fecha_alta,
        univ.fecha_alta_7,
        univ.fecha_alta_15,
        univ.fecha_alta_30,
        dat.vol_total_2g,
        dat.vol_total_3g,
        dat.vol_total_lte,
        dat.vol_total_otro,
        dat.activity_start_dt as fecha_proceso
    FROM db_cmd.otc_t_dm_cur_t2 dat 
    LEFT JOIN universo_trafico_captacion univ
    ON dat.numeroorigen=univ.telefono
    WHERE activity_start_dt >= {FECHA_INICIO} 
        AND activity_start_dt <= {FECHA_FIN}
        AND numeroorigen IN (SELECT DISTINCT telefono FROM universo_trafico_captacion)
    """.format(FECHA_INICIO=FECHA_INICIO, 
            FECHA_FIN=FECHA_FIN)
    return qry

def q_generar_reporte_trafico_datos_ventana(FECHA_INICIO, FECHA_FIN):
    qry = """
    SELECT telefono,
        vol_total_2g,
        vol_total_3g,
        vol_total_lte,
        vol_total_otro,
        fecha_proceso,
        tipo_movimiento,
        fecha_movimiento,
        segmento,
        fecha_alta,
        fecha_alta_7,
        fecha_alta_15,
        fecha_alta_30
    FROM reporte_datos
    WHERE fecha_proceso >= {FECHA_INICIO}
        AND fecha_proceso <= {FECHA_FIN}
    """.format(FECHA_INICIO=FECHA_INICIO, 
            FECHA_FIN=FECHA_FIN)
    return qry

def q_generar_reporte_trafico_datos_resumido(TABLA):
    qry = """
    SELECT telefono,
        CAST(SUM(COALESCE(vol_total_2g, 0)/1024/1024) AS decimal(19, 2)) AS total_trafico_2g,
        CAST(SUM(COALESCE(vol_total_3g, 0)/1024/1024) AS decimal(19, 2)) AS total_trafico_3g,
        CAST(SUM(COALESCE(vol_total_lte, 0)/1024/1024) AS decimal(19, 2)) AS total_trafico_lte,
        CAST(SUM(COALESCE(vol_total_otro, 0)/1024/1024) AS decimal(19, 2)) AS total_trafico_otro
    FROM {TABLA}
    GROUP BY telefono
    """.format(TABLA=TABLA)
    return qry

def q_generar_reporte_trafico_voz(COLUMNA, FECHA_INICIO, FECHA_FIN):
    qry = """
    SELECT {COLUMNA} AS telefono,
        duracion,
        fecha_evento_int as fecha_proceso
    FROM db_trafica.otc_t_cur_voz_trafica 
    WHERE fecha_evento_int >= {FECHA_INICIO} 
        AND fecha_evento_int <= {FECHA_FIN}
        AND {COLUMNA} IN (SELECT DISTINCT telefono FROM universo_trafico_captacion)
    """.format(COLUMNA=COLUMNA,
        FECHA_INICIO=FECHA_INICIO,
        FECHA_FIN=FECHA_FIN)
    return qry

def q_generar_reporte_trafico_voz_resumido(TABLA, COLUMNA_FECHA_INICIO, COLUMNA_FECHA_FIN, sentido, DIAS):
    qry = """
    SELECT telefono,
        SUM(ABS(CAST(COALESCE(duracion, 0) AS int))) AS trafico_{sentido}_voz_{DIAS}
    FROM {TABLA}
    WHERE fecha_proceso >= {COLUMNA_FECHA_INICIO} 
        AND fecha_proceso <= {COLUMNA_FECHA_FIN}
	GROUP BY telefono
    """.format(
        TABLA=TABLA,
        DIAS=DIAS,
        sentido=sentido,
        COLUMNA_FECHA_INICIO=COLUMNA_FECHA_INICIO,
        COLUMNA_FECHA_FIN=COLUMNA_FECHA_FIN
    )
    return qry

def q_borrar_trafico_captacion_previo(vSChema, FECHA_EJECUCION_ANTERIOR):
    qry = """          
        ALTER TABLE {vSChema}.trafico_captacion DROP IF EXISTS PARTITION(fecha_proceso={FECHA_EJECUCION_ANTERIOR})
    """.format(vSChema=vSChema, FECHA_EJECUCION_ANTERIOR=FECHA_EJECUCION_ANTERIOR)
    return qry

def q_insertar_trafico_captacion(vSChema, FECHA_EJECUCION):
	qry = """
		INSERT OVERWRITE TABLE {vSChema}.trafico_captacion PARTITION(fecha_proceso={FECHA_EJECUCION})
		SELECT tipo_movimiento,
            telefono,
            fecha_movimiento,
            trafico_entrante_voz_7,
            trafico_saliente_voz_7,
            total_trafico_datos_7,
            trafico_entrante_voz_15,
            trafico_saliente_voz_15,
            total_trafico_datos_15,
            trafico_entrante_voz_30,
            trafico_saliente_voz_30,
            total_trafico_datos_30,
            segmento
		FROM {vSChema}.trafico_captacion_diario
        """.format(vSChema=vSChema, FECHA_EJECUCION=FECHA_EJECUCION)
	return qry

def q_generar_reporte_trafico_captacion(vSChema):
	qry="""
		SELECT tipo_movimiento,
            telefono,
            fecha_movimiento,
            trafico_entrante_voz_7,
            trafico_saliente_voz_7,
            total_trafico_datos_7,
            trafico_entrante_voz_15,
            trafico_saliente_voz_15,
            total_trafico_datos_15,
            trafico_entrante_voz_30,
            trafico_saliente_voz_30,
            total_trafico_datos_30,
            fecha_proceso,
            segmento
		FROM {vSChema}.trafico_captacion
        """.format(vSChema=vSChema)
	return qry
