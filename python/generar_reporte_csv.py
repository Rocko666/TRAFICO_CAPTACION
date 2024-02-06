# coding: utf-8

import sys
reload(sys)
sys.setdefaultencoding('utf8')

import os
import argparse
from datetime import datetime

from pyspark.sql import SparkSession

from query import *

sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *
from create import *

timestart = datetime.now()

vSStep = '[Paso 1]: Obteniendo parametros de la SHELL'
print(lne_dvs())
print(etq_info(vSStep))
try:
    ts_step = datetime.now()  
    parser = argparse.ArgumentParser()
    parser.add_argument('--vSEntidad', required=True, type=str, help='Entidad del proceso')
    parser.add_argument('--vSQueue', required=True, type=str, help='Coja de ejecucion')
    parser.add_argument('--vSChema', required=True, type=str, help='')
    parser.add_argument('--RUTA_CSV', required=True, type=str, help='Ruta del reporte csv')  

    parametros = parser.parse_args()
    vSEntidad = parametros.vSEntidad
    vSQueue = parametros.vSQueue
    vSChema = parametros.vSChema
    RUTA_CSV = parametros.RUTA_CSV

    print(lne_dvs())
    print(etq_info("Imprimiendo parametros..."))
    print(lne_dvs())
    print(etq_info(log_p_parametros("vSEntidad", str(vSEntidad))))
    print(etq_info(log_p_parametros("vSQueue", str(vSQueue))))
    print(etq_info(log_p_parametros("vSChema", str(vSChema))))
    print(etq_info(log_p_parametros("RUTA_CSV", str(RUTA_CSV))))

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
        config('spark.yarn.queue', vSQueue). \
        enableHiveSupport(). \
        getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    app_id = spark._sc.applicationId
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))

print(lne_dvs())
vSStep = 'Paso [3]: Generar archivo csv'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(etq_info(str(vSStep)))
    print(lne_dvs())

    vSQL = q_generar_reporte_trafico_captacion(vSChema)    
    print(etq_sql(vSQL))
    
    df0 = spark.sql(vSQL)

    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df0'))))
    else:
        vIRows = df0.count()
        print(etq_info(msg_t_total_registros_obtenidos('df0', str(vIRows))))
        
        pandas_df = df0.toPandas()
        pandas_df.to_csv(RUTA_CSV, index=False)
        
        del df0
        del pandas_df
    
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

