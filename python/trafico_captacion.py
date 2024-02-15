# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf8')

import os
import argparse
from datetime import datetime

from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession

from query import *

sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *
from create import *


def generar_reporte(vSQL):
    reporte = spark.sql(vSQL)
    return reporte


def sumar_trafico_datos(REPORTE, NUEVA):
    return REPORTE.withColumn(NUEVA, (col("total_trafico_2g") + col("total_trafico_3g") + col("total_trafico_lte") + col("total_trafico_otro")))


timestart = datetime.now()
vSStep = '[Paso 1]: Obteniendo parametros de la SHELL'
print(lne_dvs())
print(etq_info(vSStep))
try:
    ts_step = datetime.now()  
    parser = argparse.ArgumentParser()
    parser.add_argument('--vSEntidad', required=True, type=str, help='Entidad del proceso')
    parser.add_argument('--vSChema', required=True, type=str, help='')
    parser.add_argument('--vSChemaTmp', required=True, type=str, help='')
    parser.add_argument('--FECHA_EJECUCION', required=True, type=str, help='')
    parser.add_argument('--FECHA_EJECUCION_ANTERIOR', required=True, type=str, help='')
    parser.add_argument('--ELIMINAR_PARTICION_PREVIA', required=True, type=str, help='')
    parser.add_argument('--FECHA_DOS_ANIOS_ATRAS', required=True, type=str, help='')
    parser.add_argument('--FECHA_INICIO', required=True, type=str, help='')
    parser.add_argument('--FECHA_FIN_MES_PREVIO', required=True, type=str, help='')
    
    parametros = parser.parse_args()
    vSEntidad = parametros.vSEntidad
    vSChema = parametros.vSChema
    vSChemaTmp = parametros.vSChemaTmp
    FECHA_EJECUCION = parametros.FECHA_EJECUCION
    FECHA_EJECUCION_ANTERIOR = parametros.FECHA_EJECUCION_ANTERIOR
    ELIMINAR_PARTICION_PREVIA = parametros.ELIMINAR_PARTICION_PREVIA
    FECHA_DOS_ANIOS_ATRAS = parametros.FECHA_DOS_ANIOS_ATRAS
    FECHA_INICIO = parametros.FECHA_INICIO
    FECHA_FIN_MES_PREVIO = parametros.FECHA_FIN_MES_PREVIO

    print(lne_dvs())
    print(etq_info("Imprimiendo parametros..."))
    print(lne_dvs())
    print(etq_info(log_p_parametros("vSEntidad", str(vSEntidad))))
    print(etq_info(log_p_parametros("vSChema", str(vSChema))))
    print(etq_info(log_p_parametros("vSChemaTmp", str(vSChemaTmp))))
    print(etq_info(log_p_parametros("FECHA_EJECUCION", str(FECHA_EJECUCION))))
    print(etq_info(log_p_parametros("FECHA_EJECUCION_ANTERIOR", str(FECHA_EJECUCION_ANTERIOR))))
    print(etq_info(log_p_parametros("ELIMINAR_PARTICION_PREVIA", str(ELIMINAR_PARTICION_PREVIA))))
    print(etq_info(log_p_parametros("FECHA_DOS_ANIOS_ATRAS", str(FECHA_DOS_ANIOS_ATRAS))))
    print(etq_info(log_p_parametros("FECHA_INICIO", str(FECHA_INICIO))))
    print(etq_info(log_p_parametros("FECHA_FIN_MES_PREVIO", str(FECHA_FIN_MES_PREVIO))))    

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = '[Paso 2]: Configuracion Spark Session'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()    
    spark = SparkSession. \
        builder. \
        config("hive.exec.dynamic.partition.mode", "nonstrict"). \
        enableHiveSupport(). \
        getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    app_id = spark._sc.applicationId
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [3]: Generar universo altas'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    vSQL = q_generar_universo_altas(FECHA_FIN_MES_PREVIO, FECHA_EJECUCION)    
    print(etq_sql(vSQL))

    universo_altas = spark.sql(vSQL)
    universo_altas = universo_altas.select("telefono", "tipo_movimiento", "fecha_movimiento","segmento")
    universo_altas.show(3)

    if universo_altas.limit(1).count <= 0:
        exit(etq_nodata(msg_e_df_nodata(str('universo_altas'))))
    else:
        vIRows = universo_altas.count()
        print(etq_info(msg_t_total_registros_obtenidos('universo_altas', str(vIRows))))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [4]: Generar universo transferencias'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    vSQL = q_generar_universo_transferencias(FECHA_FIN_MES_PREVIO, FECHA_EJECUCION)    
    print(etq_sql(vSQL))

    universo_transferencias = spark.sql(vSQL)
    universo_transferencias.show(3)
    
    if universo_transferencias.limit(1).count <= 0:
        exit(etq_nodata(msg_e_df_nodata(str('universo_transferencias'))))
    else:
        vIRows = universo_transferencias.count()
        print(etq_info(msg_t_total_registros_obtenidos('universo_transferencias', str(vIRows))))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [5]: Generar universo trafico captacion'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    universo_trafico_captacion = universo_altas.union(universo_transferencias)
    FECHA_INICIO_FORMATEADA = datetime.strptime(str(FECHA_INICIO), '%Y%m%d')
    universo_trafico_captacion = universo_trafico_captacion.where(col("fecha_movimiento") >= FECHA_INICIO_FORMATEADA)

    if universo_trafico_captacion.limit(1).count <= 0:    
        exit(etq_nodata(msg_e_df_nodata(str('universo_trafico_captacion'))))
    else:
        vIRows = universo_trafico_captacion.count()
        print(etq_info(msg_t_total_registros_obtenidos('universo', str(vIRows))))
       
        universo_trafico_captacion.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.universo_trafico_captacion".format(vSChemaTmp))
        lineas_universo_altas = tuple(universo_trafico_captacion.select(
            "telefono").rdd.map(lambda x: int(x[0])).collect()
        )

        del universo_altas
        del universo_transferencias
        del universo_trafico_captacion

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [5.1]: Generar universo trafico captacion'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    vSQL = q_generar_universo_trafico_captacion(vSChemaTmp)    
    print(etq_sql(vSQL))

    universo_trafico_captacion = spark.sql(vSQL)
    universo_trafico_captacion.show(3)
    
    if universo_trafico_captacion.limit(1).count <= 0:    
        exit(etq_nodata(msg_e_df_nodata(str('universo_trafico_captacion'))))
    else:
        vIRows = universo_trafico_captacion.count()
        print(etq_info(msg_t_total_registros_obtenidos('universo', str(vIRows))))
       
        universo_trafico_captacion.createOrReplaceTempView("universo_trafico_captacion")

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [5.2]: Generar ventanas moviles'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    vSQL = q_generar_ventanas_moviles()
    print(etq_sql(vSQL))

    ventanas_moviles = spark.sql(vSQL)
    ventanas_moviles.show(3)

    data_dict = ventanas_moviles.first().asDict()
    FECHA_ALTA_PLUS_7 = data_dict["fecha_alta_7"]
    FECHA_ALTA_PLUS_15 = data_dict["fecha_alta_15"]
    FECHA_ALTA_PLUS_30 = data_dict["fecha_alta_30"]
    
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [6]: Generar reporte datos'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    # se crea el reporte de trafico datos para 7 dias
    vSQL = q_generar_reporte_trafico_datos(vSChemaTmp, FECHA_INICIO, FECHA_ALTA_PLUS_7, lineas_universo_altas)    
    reporte_datos_7_dias = spark.sql(vSQL)
    #print(etq_sql(vSQL))

    reporte_datos_7_dias = reporte_datos_7_dias.join(universo_trafico_captacion, on="telefono", how="left")
    reporte_datos_7_dias.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.reporte_datos_7_dias".format(vSChemaTmp))
    
    vSQL = q_generar_reporte_trafico_datos_resumido(vSChemaTmp, "reporte_datos_7_dias", "fecha_alta", "fecha_alta_7")    
    reporte_datos_7_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    # se crea el reporte de trafico datos para 15 dias
    vSQL = q_generar_reporte_trafico_datos(vSChemaTmp, FECHA_INICIO, FECHA_ALTA_PLUS_15, lineas_universo_altas)    
    reporte_datos_15_dias = spark.sql(vSQL)
    #print(etq_sql(vSQL))

    reporte_datos_15_dias = reporte_datos_15_dias.join(universo_trafico_captacion, on="telefono", how="left")
    reporte_datos_15_dias.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.reporte_datos_15_dias".format(vSChemaTmp))
    
    vSQL = q_generar_reporte_trafico_datos_resumido(vSChemaTmp, "reporte_datos_15_dias", "fecha_alta", "fecha_alta_15")    
    reporte_datos_15_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    # se crea el reporte de trafico datos para 30 dias
    vSQL = q_generar_reporte_trafico_datos(vSChemaTmp, FECHA_INICIO, FECHA_ALTA_PLUS_30, lineas_universo_altas)    
    reporte_datos_30_dias = spark.sql(vSQL)
    #print(etq_sql(vSQL))

    reporte_datos_30_dias = reporte_datos_30_dias.join(universo_trafico_captacion, on="telefono", how="left")
    reporte_datos_30_dias.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.reporte_datos_30_dias".format(vSChemaTmp))
    
    vSQL = q_generar_reporte_trafico_datos_resumido(vSChemaTmp, "reporte_datos_30_dias", "fecha_alta", "fecha_alta_30")    
    reporte_datos_30_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    # se suman las 4 columnas de trafico datos
    reporte_datos_7_dias = sumar_trafico_datos(reporte_datos_7_dias, "total_trafico_datos_7")
    reporte_datos_15_dias = sumar_trafico_datos(reporte_datos_15_dias, "total_trafico_datos_15")
    reporte_datos_30_dias = sumar_trafico_datos(reporte_datos_30_dias, "total_trafico_datos_30")
    
    # se crea el reporte de trafico datos
    reporte_trafico_datos = reporte_datos_30_dias.join(reporte_datos_15_dias, on="telefono", how="left")
    reporte_trafico_datos = reporte_trafico_datos.join(reporte_datos_7_dias, on="telefono", how="left")
    reporte_trafico_datos = reporte_trafico_datos.select("telefono", "total_trafico_datos_7", "total_trafico_datos_15", "total_trafico_datos_30")
    
    if reporte_trafico_datos.limit(1).count <= 0:    
        exit(etq_nodata(msg_e_df_nodata(str('reporte_trafico_datos'))))
    else:
        vIRows = reporte_trafico_datos.count()
        print(etq_info(msg_t_total_registros_obtenidos('reporte_trafico_datos', str(vIRows))))
        
        # reporte_trafico_datos.write.mode("overwrite").saveAsTable("{}.trafico_captacion_datos".format(vSChemaTmp))

        del reporte_datos_7_dias
        del reporte_datos_15_dias
        del reporte_datos_30_dias
        # del reporte_trafico_datos

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [7]: Generar reporte saliente voz'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    sentido = "SALIENTE"

    vSQL = q_generar_reporte_trafico_voz(vSChemaTmp, "a_direction_number", FECHA_INICIO, FECHA_ALTA_PLUS_30, lineas_universo_altas)
    reporte_voz_30_dias = generar_reporte(vSQL)
    #print(etq_sql(vSQL))

    reporte_voz_30_dias = reporte_voz_30_dias.join(universo_trafico_captacion, on="telefono", how="left")
    reporte_voz_30_dias.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.reporte_voz_saliente_30".format(vSChemaTmp))

    # se crea el reporte de trafico saliente voz para 7 dias    
    vSQL = q_generar_reporte_trafico_voz_resumido(vSChemaTmp, "reporte_voz_saliente_30", "fecha_alta", "fecha_alta_7", sentido.lower(), "7")    
    reporte_voz_7_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    # se crea el reporte de trafico saliente voz para 15 dias
    vSQL = q_generar_reporte_trafico_voz_resumido(vSChemaTmp, "reporte_voz_saliente_30", "fecha_alta", "fecha_alta_15", sentido.lower(), "15")    
    reporte_voz_15_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    # se crea el reporte de trafico saliente voz para 30 dias
    vSQL = q_generar_reporte_trafico_voz_resumido(vSChemaTmp, "reporte_voz_saliente_30", "fecha_alta", "fecha_alta_30", sentido.lower(), "30")    
    reporte_voz_30_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    # se crea el reporte trafico saliente voz
    reporte_saliente_voz = reporte_voz_30_dias.join(reporte_voz_15_dias, on="telefono", how="left")
    reporte_saliente_voz = reporte_saliente_voz.join(reporte_voz_7_dias, on="telefono", how="left")
    
    if reporte_saliente_voz.limit(1).count <= 0:    
        exit(etq_nodata(msg_e_df_nodata(str('reporte_saliente_voz'))))
    else:
        vIRows = reporte_saliente_voz.count()
        print(etq_info(msg_t_total_registros_obtenidos('reporte_saliente_voz', str(vIRows))))
        
        # reporte_saliente_voz.write.mode("overwrite").saveAsTable("{}.trafico_captacion_saliente_voz".format(vSChemaTmp))

        del reporte_voz_7_dias
        del reporte_voz_15_dias
        del reporte_voz_30_dias
        # del reporte_saliente_voz

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [8]: Generar reporte entrante voz'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    sentido = "ENTRANTE"

    vSQL = q_generar_reporte_trafico_voz(vSChemaTmp, "b_direction_number", FECHA_INICIO, FECHA_ALTA_PLUS_30, lineas_universo_altas)
    reporte_voz_30_dias = generar_reporte(vSQL)
    #print(etq_sql(vSQL))

    reporte_voz_30_dias = reporte_voz_30_dias.join(universo_trafico_captacion, on="telefono", how="left")
    reporte_voz_30_dias.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.reporte_voz_entrante_30".format(vSChemaTmp))

    # se crea el reporte de trafico entrante voz para 7 dias
    vSQL = q_generar_reporte_trafico_voz_resumido(vSChemaTmp, "reporte_voz_entrante_30", "fecha_alta", "fecha_alta_7", sentido.lower(), "7")    
    reporte_voz_7_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    # se crea el reporte de trafico entrante voz para 15 dias
    vSQL = q_generar_reporte_trafico_voz_resumido(vSChemaTmp, "reporte_voz_entrante_30", "fecha_alta", "fecha_alta_15", sentido.lower(), "15")    
    reporte_voz_15_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    # se crea el reporte de trafico entrante voz para 30 dias
    vSQL = q_generar_reporte_trafico_voz_resumido(vSChemaTmp, "reporte_voz_entrante_30", "fecha_alta", "fecha_alta_30", sentido.lower(), "30")    
    reporte_voz_30_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))
    
    # se crea el reporte trafico entrante voz
    reporte_entrante_voz = reporte_voz_30_dias.join(reporte_voz_15_dias, on="telefono", how="left")    
    reporte_entrante_voz = reporte_entrante_voz.join(reporte_voz_7_dias, on="telefono", how="left")
    
    if reporte_entrante_voz.limit(1).count <= 0:    
        exit(etq_nodata(msg_e_df_nodata(str('reporte_entrante_voz'))))
    else:
        vIRows = reporte_entrante_voz.count()
        print(etq_info(msg_t_total_registros_obtenidos('reporte_entrante_voz', str(vIRows))))
        
        # reporte_entrante_voz.write.mode("overwrite").saveAsTable("{}.trafico_captacion_entrante_voz".format(vSChemaTmp))
        
        del reporte_voz_7_dias
        del reporte_voz_15_dias
        del reporte_voz_30_dias
        # del reporte_entrante_voz

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [9]: Eliminar reportes previos'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    if ELIMINAR_PARTICION_PREVIA == "SI":
        try:
            vSQL = q_borrar_trafico_captacion_previo(vSChemaTmp, FECHA_EJECUCION_ANTERIOR)
            spark.sql(vSQL)
            print(etq_sql(vSQL))
        except:
            print("Particion no encontrada")
            pass
        
        try:
            vSQL = q_borrar_trafico_captacion_previo(vSChemaTmp, FECHA_DOS_ANIOS_ATRAS)
            spark.sql(vSQL)
            print(etq_sql(vSQL))
        except:
            print("Particion no encontrada")
            pass

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [10]: Generar reporte Trafico Captacion'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    # se crea el reporte trafico captacion pospago
    reporte_trafico_captacion = universo_trafico_captacion.join(reporte_entrante_voz, on="telefono", how="left")
    reporte_trafico_captacion = reporte_trafico_captacion.join(reporte_saliente_voz, on="telefono", how="left")
    reporte_trafico_captacion = reporte_trafico_captacion.join(reporte_trafico_datos, on="telefono", how="left")
    
    # se adjunta la fecha de ejecucion al reporte
    reporte_trafico_captacion = reporte_trafico_captacion.withColumn("fecha_proceso", lit(FECHA_EJECUCION))

    # se reemplazan posibles valores nulos
    reporte_trafico_captacion = reporte_trafico_captacion.na.fill(value=0)

    if reporte_trafico_captacion.limit(1).count <= 0:        
        exit(etq_nodata(msg_e_df_nodata(str('reporte_trafico_captacion'))))
    else:
        vIRows = reporte_trafico_captacion.count()
        print(etq_info(msg_t_total_registros_obtenidos('reporte_trafico_captacion', str(vIRows))))

        reporte_trafico_captacion.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.trafico_captacion_diario".format(vSChema))

        vSQL = q_insertar_trafico_captacion(vSChema, FECHA_EJECUCION)
        spark.sql(vSQL)
        print(etq_sql(vSQL))

        del reporte_entrante_voz
        del reporte_saliente_voz
        del reporte_trafico_datos
        del reporte_trafico_captacion

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad, vle_duracion(timestart, timeend))))
print(lne_dvs())
