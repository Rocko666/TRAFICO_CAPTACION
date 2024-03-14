#########################################################################################################
# NOMBRE: SH_TRAFICO_CAPTACION    		      							                                #
# DESCRIPCION: Shell principal genera un reporte de trafico captacion pospago, usando como fuente las   #
# tablas db_cs_altas.otc_t_altas_bi, db_cs_altas.otc_t_transfer_in_bi para definir el universo de       #
# lineas a analizar y las tablas db_cmd.otc_t_dm_cur_t2 y db_trafica.otc_t_cur_voz_trafica para generar #
# el trafico de datos y voz de cada linea                                                               #
# AUTOR: Cesar Andrade - Softconsulting                            						                #
# FECHA CREACION: 2023-06-30   											                                #
# PARAMETROS DEL SHELL                            								                        #
#########################################################################################################
# MODIFICACIONES													                                    #
# FECHA  		AUTOR     		DESCRIPCION MOTIVO							                            #
# YYYY-MM-DD    NOMBRE			                                                     	  	            #
#########################################################################################################
set -e

#------------------------------------------------------
# PARAMETROS DE LA SHELL
#------------------------------------------------------
VAL_FECHA_EJECUCION=$1

ENTIDAD=D_RPRTTRFCCPTCN0010
AMBIENTE=0 # AMBIENTE (1=produccion, 0=desarrollo)

if [ $AMBIENTE -gt 0 ]; then
    TABLA=params
else
    TABLA=params_des
fi

#------------------------------------------------------
# PARAMETROS DE LA TABLA PARAMS
#------------------------------------------------------
VAL_RUTA=$(mysql -N <<<"select valor from $TABLA where entidad = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';")
VAL_LOCAL_RUTA_OUT=$(mysql -N <<<"select valor from $TABLA where entidad = '"$ENTIDAD"' AND parametro = 'VAL_LOCAL_RUTA_OUT';")
VAL_NOM_FILE_OUT=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOM_FILE_OUT';")
VAL_ENCODING=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ENCODING';")
ESQUEMA=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ESQUEMA';")
ESQUEMA_TMP=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ESQUEMA_TMP';")
ETAPA=$(mysql -N <<<"select valor from $TABLA where entidad = '"$ENTIDAD"' AND parametro = 'ETAPA';")
VAL_COLA_EJECUCION=$(mysql -N <<<"select valor from $TABLA where entidad = '"$ENTIDAD"' AND parametro = 'VAL_COLA_EJECUCION';")

#------------------------------------------------------
# PARAMETROS SPARK
#------------------------------------------------------
VAL_RUTA_SPARK=$(mysql -N <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';")
VAL_MASTER=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';")
VAL_DRIVER_MEMORY=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';")
VAL_EXECUTOR_MEMORY=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';")
VAL_NUM_EXECUTORS=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';")
VAL_NUM_EXECUTORS_CORES=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES';")
VAL_KINIT=$(mysql -N <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';")
$VAL_KINIT

#------------------------------------------------------
# PARAMETROS ACCESO SFTP
#------------------------------------------------------
SFTP_GENERICO_SH=$(mysql -N <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'SFTP_GENERICO_SH';")
VAL_SFTP_HOST_OUT=$(mysql -N <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_HOST';")
VAL_SFTP_PORT_OUT=$(mysql -N <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_PORT';")
VAL_SFTP_USER_OUT=$(mysql -N <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_USER_DDATOS';")
VAL_SFTP_PASS_OUT=$(mysql -N <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_PASS_DDATOS';")
VAL_SFTP_RUTA_OUT=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_RUTA_OUT';")

#------------------------------------------------------
# PARAMETROS CALCULADOS
#------------------------------------------------------
VAL_MODO=0
VAL_BANDERA_FTP=0
ini_fecha=$(date '+%Y%m%d%H%M%S')
VAL_LOG=$VAL_RUTA/logs/trafico_captacion_$ini_fecha.log
VAL_ELIM_PART_PREVIA="SI"
VAL_FECHA_EJECUCION_ANTERIOR=$(date '+%Y%m%d' -d "$VAL_FECHA_EJECUCION-1 day")
VAL_DIA_EJE=$(date '+%d' -d "$VAL_FECHA_EJECUCION")

if [ $VAL_DIA_EJE = "01" ]; then
    VAL_FECHA_DOS_ANIOS_ATRAS=$(date '+%Y%m%d' -d "$VAL_FECHA_EJECUCION-24 month")
    VAL_FECHA_INI_MES=$(date -d "$VAL_FECHA_EJECUCION -1 month" +%Y%m%d)
    VAL_FECHA_INI_2MES=$(date -d "$VAL_FECHA_EJECUCION -2 month" +%Y%m%d)
else
    VAL_FECHA_INI_MES=$(date -d "$VAL_FECHA_EJECUCION" +%Y%m)"01"
    VAL_FECHA_INI_2MES=$(date -d "$VAL_FECHA_EJECUCION -1 month" +%Y%m)"01"
fi

if [ $VAL_DIA_EJE = "02" ]; then
    VAL_ELIM_PART_PREVIA="NO"
fi

#------------------------------------------------------
# VALIDACION DE PARAMETROS
#------------------------------------------------------
if [ -z "$VAL_FECHA_EJECUCION" ] ||
    [ -z "$VAL_FECHA_DOS_ANIOS_ATRAS" ] ||
    [ -z "$VAL_FECHA_EJECUCION_ANTERIOR" ] ||
    [ -z "$VAL_ELIM_PART_PREVIA" ] ||
    [ -z "$VAL_DIA_EJE" ] ||
    [ -z "$VAL_FECHA_INI_MES" ] ||
    [ -z "$VAL_FECHA_INI_2MES" ] ||
    [ -z "$VAL_RUTA" ] ||
    [ -z "$VAL_LOCAL_RUTA_OUT" ] ||
    [ -z "$VAL_NOM_FILE_OUT" ] ||
    [ -z "$VAL_ENCODING" ] ||
    [ -z "$ESQUEMA" ] ||
    [ -z "$ESQUEMA_TMP" ] ||
    [ -z "$ETAPA" ] ||
    [ -z "$VAL_COLA_EJECUCION" ] ||
    [ -z "$VAL_RUTA_SPARK" ] ||
    [ -z "$VAL_MASTER" ] ||
    [ -z "$VAL_DRIVER_MEMORY" ] ||
    [ -z "$VAL_EXECUTOR_MEMORY" ] ||
    [ -z "$VAL_NUM_EXECUTORS" ] ||
    [ -z "$VAL_NUM_EXECUTORS_CORES" ] ||
    [ -z "$VAL_KINIT" ] ||
    [ -z "$SFTP_GENERICO_SH" ] ||
    [ -z "$VAL_SFTP_HOST_OUT" ] ||
    [ -z "$VAL_SFTP_PORT_OUT" ] ||
    [ -z "$VAL_SFTP_USER_OUT" ] ||
    [ -z "$VAL_SFTP_PASS_OUT" ] ||
    [ -z "$VAL_SFTP_RUTA_OUT" ] ||
    [ -z "$VAL_LOG" ]; then
    echo " ERROR - uno de los parametros esta vacio o nulo"
    exit 1
fi

#------------------------------------------------------
# IMPRESION PARAMETROS
#------------------------------------------------------
echo "VAL_FECHA_EJECUCION: $VAL_FECHA_EJECUCION" >>$VAL_LOG
echo "VAL_RUTA: $VAL_RUTA" >>$VAL_LOG
echo "VAL_LOCAL_RUTA_OUT: $VAL_LOCAL_RUTA_OUT" >>$VAL_LOG
echo "VAL_NOM_FILE_OUT: $VAL_NOM_FILE_OUT" >>$VAL_LOG
echo "VAL_ENCODING: $VAL_ENCODING" >>$VAL_LOG
echo "ESQUEMA: $ESQUEMA" >>$VAL_LOG
echo "ESQUEMA_TMP: $ESQUEMA_TMP" >>$VAL_LOG
echo "ETAPA: $ETAPA" >>$VAL_LOG
echo "VAL_COLA_EJECUCION: $VAL_COLA_EJECUCION" >>$VAL_LOG
echo "VAL_RUTA_SPARK: $VAL_RUTA_SPARK" >>$VAL_LOG
echo "VAL_MASTER: $VAL_MASTER" >>$VAL_LOG
echo "VAL_DRIVER_MEMORY: $VAL_DRIVER_MEMORY" >>$VAL_LOG
echo "VAL_EXECUTOR_MEMORY: $VAL_EXECUTOR_MEMORY" >>$VAL_LOG
echo "VAL_NUM_EXECUTORS: $VAL_NUM_EXECUTORS" >>$VAL_LOG
echo "VAL_NUM_EXECUTORS_CORES: $VAL_NUM_EXECUTORS_CORES" >>$VAL_LOG
echo "VAL_KINIT: $VAL_KINIT" >>$VAL_LOG
echo "SFTP_GENERICO_SH: $SFTP_GENERICO_SH" >>$VAL_LOG
echo "VAL_SFTP_HOST_OUT: $VAL_SFTP_HOST_OUT" >>$VAL_LOG
echo "VAL_SFTP_PORT_OUT: $VAL_SFTP_PORT_OUT" >>$VAL_LOG
echo "VAL_SFTP_USER_OUT: $VAL_SFTP_USER_OUT" >>$VAL_LOG
echo "VAL_SFTP_PASS_OUT: $VAL_SFTP_PASS_OUT" >>$VAL_LOG
echo "VAL_SFTP_RUTA_OUT: $VAL_SFTP_RUTA_OUT" >>$VAL_LOG
echo "VAL_ELIM_PART_PREVIA: $VAL_ELIM_PART_PREVIA" >>$VAL_LOG
echo "VAL_FECHA_EJECUCION_ANTERIOR: $VAL_FECHA_EJECUCION_ANTERIOR" >>$VAL_LOG
echo "VAL_FECHA_DOS_ANIOS_ATRAS: $VAL_FECHA_DOS_ANIOS_ATRAS" >>$VAL_LOG
echo "VAL_FECHA_INI_MES: $VAL_FECHA_INI_MES" >>$VAL_LOG

#------------------------------------------------------
# GENERACION DE REPORTE TRAFICO CAPTACION
#------------------------------------------------------
if [ $ETAPA = 1 ]; then

    $VAL_RUTA_SPARK \
        --name $ENTIDAD \
        --queue $VAL_COLA_EJECUCION \
        --conf spark.port.maxRetries=100 \
        --master $VAL_MASTER \
        --driver-memory $VAL_DRIVER_MEMORY \
        --executor-memory $VAL_EXECUTOR_MEMORY \
        --num-executors $VAL_NUM_EXECUTORS \
        --executor-cores $VAL_NUM_EXECUTORS_CORES \
        $VAL_RUTA/python/trafico_captacion.py \
        --vSEntidad=$ENTIDAD \
        --vSChema=$ESQUEMA \
        --vSChemaTmp=$ESQUEMA_TMP \
        --VAL_FECHA_EJECUCION=$VAL_FECHA_EJECUCION \
        --VAL_FECHA_EJECUCION_ANTERIOR=$VAL_FECHA_EJECUCION_ANTERIOR \
        --VAL_ELIM_PART_PREVIA=$VAL_ELIM_PART_PREVIA \
        --VAL_FECHA_DOS_ANIOS_ATRAS=$VAL_FECHA_DOS_ANIOS_ATRAS \
        --VAL_FECHA_INI_MES=$VAL_FECHA_INI_MES \
        --VAL_FECHA_INI_2MES=$VAL_FECHA_INI_2MES 2>&1 &>> $VAL_LOG

    echo "==== FIN PROCESO GENERACION DE REPORTE TRAFICO CAPTACION ====" >>$VAL_LOG

    # seteo de etapa
    echo "Procesado ETAPA 1" &>>$VAL_LOG
    $(mysql -N <<<"update $TABLA set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';")
    ETAPA=2
fi

#------------------------------------------------------
# GENERACION DE REPORTE CSV
#------------------------------------------------------
if [ $ETAPA = 2 ]; then

    $VAL_RUTA_SPARK \
        --conf spark.port.maxRetries=100 \
        --master $VAL_MASTER \
        --name $ENTIDAD \
        --driver-memory $VAL_DRIVER_MEMORY \
        --executor-memory $VAL_EXECUTOR_MEMORY \
        --num-executors $VAL_NUM_EXECUTORS \
        --executor-cores $VAL_NUM_EXECUTORS_CORES \
        $VAL_RUTA/python/generar_reporte_csv.py \
        --vSEntidad=$ENTIDAD \
        --vSQueue=$VAL_COLA_EJECUCION \
        --vSChema=$ESQUEMA \
        --RUTA_CSV=$VAL_LOCAL_RUTA_OUT/$VAL_NOM_FILE_OUT 2>&1 &>> $VAL_LOG

    echo "==== FIN PROCESO GENERACION DE REPORTE CSV ====" >>$VAL_LOG

    # seteo de etapa
    echo "Procesado ETAPA 2" &>>$VAL_LOG
    $(mysql -N <<<"update $TABLA set valor='3' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';")
    ETAPA=3
fi

if [ $ETAPA = 3 ]; then
    VAL_SFTP_RUTA_OUT=$(echo $VAL_SFTP_RUTA_OUT | sed "s/~}</ /g")
    VAL_SFTP_RUTA_OUT=$(echo $VAL_SFTP_RUTA_OUT | tr '"' "'")

    sh -x $SFTP_GENERICO_SH \
        $VAL_MODO $VAL_BANDERA_FTP \
        $VAL_SFTP_USER_OUT $VAL_SFTP_PASS_OUT $VAL_SFTP_HOST_OUT $VAL_SFTP_PORT_OUT "${VAL_SFTP_RUTA_OUT}"  \
        $VAL_NOM_FILE_OUT $VAL_LOCAL_RUTA_OUT $VAL_LOG

	VAL_ERRORES=$(egrep 'ERROR - En la transferencia del archivo' $VAL_LOG | wc -l)

	if [ $VAL_ERRORES -ne 0 ]; then
		echo "==== ERROR en la transferencia FTP ====" >>$VAL_LOG
		exit 1
	else
		echo "==== FIN PROCESO EXPORTACION FTP ====" >>$VAL_LOG
	fi

    # seteo de etapa
    echo "Procesado ETAPA 3" &>>$VAL_LOG
    $(mysql -N <<<"update $TABLA set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';")
fi

echo "==== FIN PROCESO TRAFICO CAPTACION ====" >>$VAL_LOG
