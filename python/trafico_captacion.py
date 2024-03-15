# -- coding: utf-8 --
import sys
reload(sys)
sys.setdefaultencoding('utf8')
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
    parser.add_argument('--VAL_FECHA_EJECUCION', required=True, type=str, help='')
    parser.add_argument('--VAL_FECHA_EJECUCION_ANTERIOR', required=True, type=str, help='')
    parser.add_argument('--VAL_ELIM_PART_PREVIA', required=True, type=str, help='')
    parser.add_argument('--VAL_FECHA_DOS_ANIOS_ATRAS', required=True, type=str, help='')
    parser.add_argument('--VAL_FECHA_INI_MES', required=True, type=str, help='')
    parser.add_argument('--VAL_FECHA_INI_2MES', required=True, type=str, help='')
    
    parametros = parser.parse_args()
    vSEntidad = parametros.vSEntidad
    vSChema = parametros.vSChema
    vSChemaTmp = parametros.vSChemaTmp
    vFEje = parametros.VAL_FECHA_EJECUCION
    vFEjeAnt = parametros.VAL_FECHA_EJECUCION_ANTERIOR
    vElimPartPre = parametros.VAL_ELIM_PART_PREVIA
    vF2AniosAtras = parametros.VAL_FECHA_DOS_ANIOS_ATRAS
    vFIniMes = parametros.VAL_FECHA_INI_MES
    vFIni2Mes = parametros.VAL_FECHA_INI_2MES

    print(lne_dvs())
    print(etq_info("Imprimiendo parametros..."))
    print(lne_dvs())
    print(etq_info(log_p_parametros("vSEntidad", str(vSEntidad))))
    print(etq_info(log_p_parametros("vSChema", str(vSChema))))
    print(etq_info(log_p_parametros("vSChemaTmp", str(vSChemaTmp))))
    print(etq_info(log_p_parametros("vFEje", str(vFEje))))
    print(etq_info(log_p_parametros("vFEjeAnt", str(vFEjeAnt))))
    print(etq_info(log_p_parametros("vElimPartPre", str(vElimPartPre))))
    print(etq_info(log_p_parametros("vF2AniosAtras", str(vF2AniosAtras))))
    print(etq_info(log_p_parametros("vFIniMes", str(vFIniMes))))       
    print(etq_info(log_p_parametros("vFIni2Mes", str(vFIni2Mes))))   

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
    vSQL = q_generar_universo_altas(vFIniMes, vFEje)    
    print(etq_sql(vSQL))

    universo_altas = spark.sql(vSQL)
    universo_altas = universo_altas.where(col("orden") == '1')
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

    vSQL = q_generar_universo_transferencias(vFIniMes, vFEje)    
    print(etq_sql(vSQL))

    universo_transferencias = spark.sql(vSQL)
    universo_transferencias = universo_transferencias.where(col('orden')=='1')
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

    if universo_trafico_captacion.limit(1).count <= 0:    
        exit(etq_nodata(msg_e_df_nodata(str('universo_trafico_captacion'))))
    else:
        vIRows = universo_trafico_captacion.count()
        print(etq_info(msg_t_total_registros_obtenidos('universo', str(vIRows))))
        universo_trafico_captacion.createOrReplaceTempView("universo_trafico_captacion")
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

    vSQL = q_generar_universo_trafico_captacion('universo_trafico_captacion')    
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

    vSQL = q_generar_ventanas_moviles(vFEje)
    print(etq_sql(vSQL))

    universo_trafico_captacion = spark.sql(vSQL)
    universo_trafico_captacion.show(3)
    universo_trafico_captacion.createOrReplaceTempView("universo_trafico_captacion")
    #universo_trafico_captacion.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.universo_ventanas_moviles".format(vSChemaTmp))
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

    # se crea el reporte de trafico datos desde inicio de mes hasta la fecha de ejecucion
    vSQL = q_generar_reporte_trafico_datos(vFIni2Mes, vFEje)    
    reporte_datos = spark.sql(vSQL)
    print('Se crea el reporte de trafico datos desde inicio de mes hasta la fecha de ejecucion:')
    print(etq_sql(vSQL))
    #reporte_datos.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.reporte_datos".format(vSChemaTmp))
    reporte_datos.createOrReplaceTempView("reporte_datos")
    
    # se crea el reporte de trafico datos para 7 dias
    vSQL = q_generar_reporte_trafico_datos_ventana("fecha_alta","fecha_alta_7")    
    print('Se crea el reporte de trafico datos para 7 dias:')
    reporte_datos_7_dias = spark.sql(vSQL)
    reporte_datos_7_dias.createOrReplaceTempView("reporte_datos_7_dias") 
    print(etq_sql(vSQL))

    vSQL = q_generar_reporte_trafico_datos_resumido("reporte_datos_7_dias")    
    reporte_datos_7_dias = spark.sql(vSQL)
    #reporte_datos_7_dias.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.reporte_datos_7_dias".format(vSChemaTmp))   
    print(etq_sql(vSQL))

    # se crea el reporte de trafico datos para 15 dias
    vSQL = q_generar_reporte_trafico_datos_ventana("fecha_alta","fecha_alta_15")  
    print('Se crea el reporte de trafico datos para 15 dias:')    
    reporte_datos_15_dias = spark.sql(vSQL)
    reporte_datos_15_dias.createOrReplaceTempView("reporte_datos_15_dias")
    print(etq_sql(vSQL))
    
    vSQL = q_generar_reporte_trafico_datos_resumido("reporte_datos_15_dias")    
    reporte_datos_15_dias = spark.sql(vSQL)
    #reporte_datos_15_dias.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.reporte_datos_15_dias".format(vSChemaTmp))    
    print(etq_sql(vSQL))

    # se crea el reporte de trafico datos para 30 dias
    vSQL = q_generar_reporte_trafico_datos_ventana("fecha_alta","fecha_alta_30")   
    print('Se crea el reporte de trafico datos para 30 dias:')
    reporte_datos_30_dias = spark.sql(vSQL)
    reporte_datos_30_dias.createOrReplaceTempView("reporte_datos_30_dias")
    print(etq_sql(vSQL))
    
    vSQL = q_generar_reporte_trafico_datos_resumido("reporte_datos_30_dias")    
    reporte_datos_30_dias = spark.sql(vSQL)
    #reporte_datos_30_dias.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.reporte_datos_30_dias".format(vSChemaTmp))    
    print(etq_sql(vSQL))

    #Eliminacion DF's y Vistas
    spark.catalog.dropTempView("reporte_datos") 
    spark.catalog.dropTempView("reporte_datos_7_dias") 
    spark.catalog.dropTempView("reporte_datos_15_dias") 
    spark.catalog.dropTempView("reporte_datos_30_dias") 

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
        
        #reporte_trafico_datos.write.mode("overwrite").saveAsTable("{}.trafico_captacion_datos".format(vSChemaTmp))

        del reporte_datos_7_dias
        del reporte_datos_15_dias
        del reporte_datos_30_dias

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

    vSQL = q_generar_reporte_trafico_voz("a_direction_number", vFIni2Mes, vFEje)
    print('Se crea el reporte de trafico saliente voz desde inicio de mes hasta la fecha de ejecucion:')
    reporte_voz_saliente = generar_reporte(vSQL)
    print(etq_sql(vSQL))

    reporte_voz_saliente = reporte_voz_saliente.join(universo_trafico_captacion, on="telefono", how="left")
    reporte_voz_saliente.createOrReplaceTempView("reporte_voz_saliente")
    #reporte_voz_saliente.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.reporte_voz_saliente".format(vSChemaTmp))

    # se crea el reporte de trafico saliente voz para 7 dias    
    vSQL = q_generar_reporte_trafico_voz_resumido("reporte_voz_saliente", "fecha_alta", "fecha_alta_7", sentido.lower(), "7")    
    print('Se crea el reporte de trafico saliente voz para 7 dias:')
    reporte_voz_7_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    # se crea el reporte de trafico saliente voz para 15 dias
    vSQL = q_generar_reporte_trafico_voz_resumido("reporte_voz_saliente", "fecha_alta", "fecha_alta_15", sentido.lower(), "15")    
    print('Se crea el reporte de trafico saliente voz para 15 dias:')
    reporte_voz_15_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    # se crea el reporte de trafico saliente voz para 30 dias
    vSQL = q_generar_reporte_trafico_voz_resumido("reporte_voz_saliente", "fecha_alta", "fecha_alta_30", sentido.lower(), "30")    
    print('Se crea el reporte de trafico saliente voz para 30 dias:')
    reporte_voz_30_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    #Eliminacion DF's y Vistas
    spark.catalog.dropTempView("reporte_voz_saliente") 

    # se crea el reporte trafico saliente voz
    reporte_saliente_voz = reporte_voz_30_dias.join(reporte_voz_15_dias, on="telefono", how="left")
    reporte_saliente_voz = reporte_saliente_voz.join(reporte_voz_7_dias, on="telefono", how="left")
    
    if reporte_saliente_voz.limit(1).count <= 0:    
        exit(etq_nodata(msg_e_df_nodata(str('reporte_saliente_voz'))))
    else:
        vIRows = reporte_saliente_voz.count()
        print(etq_info(msg_t_total_registros_obtenidos('reporte_saliente_voz', str(vIRows))))
        #reporte_saliente_voz.write.mode("overwrite").saveAsTable("{}.trafico_captacion_saliente_voz".format(vSChemaTmp))

        del reporte_voz_7_dias
        del reporte_voz_15_dias
        del reporte_voz_30_dias

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

    vSQL = q_generar_reporte_trafico_voz("b_direction_number", vFIni2Mes, vFEje)
    print('Se crea el reporte de trafico entrante voz desde inicio de mes hasta la fecha de ejecucion:')
    reporte_voz_entrante = generar_reporte(vSQL)
    print(etq_sql(vSQL))
    #Eliminacion DF's y Vistas
    spark.catalog.dropTempView("universo_trafico_captacion") 

    reporte_voz_entrante = reporte_voz_entrante.join(universo_trafico_captacion, on="telefono", how="left")
    reporte_voz_entrante.createOrReplaceTempView("reporte_voz_entrante")
    #reporte_voz_entrante.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.reporte_voz_entrante".format(vSChemaTmp))

    # se crea el reporte de trafico entrante voz para 7 dias
    vSQL = q_generar_reporte_trafico_voz_resumido("reporte_voz_entrante", "fecha_alta", "fecha_alta_7", sentido.lower(), "7")    
    print('Se crea el reporte de trafico entrante voz para 7 dias:')
    reporte_voz_7_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    # se crea el reporte de trafico entrante voz para 15 dias
    vSQL = q_generar_reporte_trafico_voz_resumido("reporte_voz_entrante", "fecha_alta", "fecha_alta_15", sentido.lower(), "15")    
    print('Se crea el reporte de trafico entrante voz para 15 dias:')
    reporte_voz_15_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    # se crea el reporte de trafico entrante voz para 30 dias
    vSQL = q_generar_reporte_trafico_voz_resumido("reporte_voz_entrante", "fecha_alta", "fecha_alta_30", sentido.lower(), "30")    
    print('Se crea el reporte de trafico entrante voz para 30 dias:')
    reporte_voz_30_dias = spark.sql(vSQL)
    print(etq_sql(vSQL))

    #Eliminacion DF's y Vistas
    spark.catalog.dropTempView("reporte_voz_entrante")
    
    # se crea el reporte trafico entrante voz
    reporte_entrante_voz = reporte_voz_30_dias.join(reporte_voz_15_dias, on="telefono", how="left")    
    reporte_entrante_voz = reporte_entrante_voz.join(reporte_voz_7_dias, on="telefono", how="left")
    
    if reporte_entrante_voz.limit(1).count <= 0:    
        exit(etq_nodata(msg_e_df_nodata(str('reporte_entrante_voz'))))
    else:
        vIRows = reporte_entrante_voz.count()
        print(etq_info(msg_t_total_registros_obtenidos('reporte_entrante_voz', str(vIRows))))
        
        #reporte_entrante_voz.write.mode("overwrite").saveAsTable("{}.trafico_captacion_entrante_voz".format(vSChemaTmp))
        
        del reporte_voz_7_dias
        del reporte_voz_15_dias
        del reporte_voz_30_dias

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

    if vElimPartPre == "SI":
        try:
            vSQL = q_borrar_trafico_captacion_previo(vSChema, vFEjeAnt)
            spark.sql(vSQL)
            print(etq_sql(vSQL))
        except:
            print("Particion no encontrada")
            pass
        
        try:
            vSQL = q_borrar_trafico_captacion_previo(vSChema, vF2AniosAtras)
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
    reporte_trafico_captacion = reporte_trafico_captacion.withColumn("fecha_proceso", lit(vFEje))

    # se reemplazan posibles valores nulos
    reporte_trafico_captacion = reporte_trafico_captacion.na.fill(value=0)

    if reporte_trafico_captacion.limit(1).count <= 0:        
        exit(etq_nodata(msg_e_df_nodata(str('reporte_trafico_captacion'))))
    else:
        vIRows = reporte_trafico_captacion.count()
        print(etq_info(msg_t_total_registros_obtenidos('reporte_trafico_captacion', str(vIRows))))

        reporte_trafico_captacion.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.trafico_captacion_diario".format(vSChema))

        vSQL = q_insertar_trafico_captacion(vSChema, vFEje)
        spark.sql(vSQL)
        print(etq_sql(vSQL))

        del reporte_entrante_voz
        del reporte_saliente_voz
        del reporte_trafico_datos

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())

print(lne_dvs())
vSStep = 'Paso [11]: Depuracion de telefonos duplicados con cierre mes anterior'
print(etq_info(vSStep))
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    # Consulta tabla trafico captacion de cierre de mes anterior
    print("Cierre mes anterior: "+ str(vFIniMes))
    vSQL = depuracion_tabla(vSChema,vFIniMes)    
    print(etq_sql(vSQL))
    df_depuracion = spark.sql(vSQL)
    # Depuracion de registros en cierre de mes anterior
    df_depuracion=df_depuracion.join(reporte_trafico_captacion, on=["telefono", "fecha_movimiento"], how="left_anti")
    #Eliminacion DF's y Vistas
    del reporte_trafico_captacion
    
    # se reemplazan posibles valores nulos
    df_depuracion = df_depuracion.na.fill(value=0)

    vIRows = df_depuracion.limit(1).count    
    if vIRows <= 0:    
        exit(etq_nodata(msg_e_df_nodata(str('DF depuracion'))))
    else:
        print(etq_info(msg_t_total_registros_obtenidos('DF depuracion', str(vIRows))))
        df_depuracion.createOrReplaceTempView("tabla_depurada")
        
        #Eliminacion DF's y Vistas
        del df_depuracion
        vSQL = q_insertar_tabla_depurada(vSChema, "tabla_depurada", vFIniMes)
        print(etq_sql(vSQL))
        spark.sql(vSQL)
        
        #Eliminacion DF's y Vistas
        spark.catalog.dropTempView("tabla_depurada") 

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
