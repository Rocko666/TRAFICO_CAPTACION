
+-------------------+-------------------------+--------------------------------------------------------------------------+-------+----------+
| ENTIDAD           | PARAMETRO               | VALOR                                                                    | ORDEN | AMBIENTE |
+-------------------+-------------------------+--------------------------------------------------------------------------+-------+----------+
| RPRTTRFCCPTCN0010 | PARAM1_FECHA_EJECUCION  | date_format(sysdate(),'%Y%m%d')                                          |     0 |        1 |
| RPRTTRFCCPTCN0010 | SHELL                   | /RGenerator/reportes/TRAFICO_CAPTACION/bin/SH_TRAFICO_CAPTACION.sh       |     0 |        1 |
| RPRTTRFCCPTCN0010 | VAL_RUTA                | /RGenerator/reportes/TRAFICO_CAPTACION                                   |     0 |        1 |
| RPRTTRFCCPTCN0010 | VAL_LOCAL_RUTA_OUT      | /RGenerator/reportes/TRAFICO_CAPTACION/output                            |     0 |        1 |
| RPRTTRFCCPTCN0010 | VAL_NOM_FILE_OUT        | captacion_pospago.csv                                                    |     0 |        1 |
| RPRTTRFCCPTCN0010 | VAL_ENCODING            | ascii                                                                    |     0 |        1 |
| RPRTTRFCCPTCN0010 | VAL_SFTP_RUTA_OUT       | "/Share/comun/gestion&proyectos/INFORMACION~}<EXTERNA/Captacion_Pos_Pre" |     0 |        1 |
| RPRTTRFCCPTCN0010 | ESQUEMA                 | db_reportes                                                              |     0 |        1 |
| RPRTTRFCCPTCN0010 | ESQUEMA_TMP             | db_temporales                                                            |     0 |        1 |
| RPRTTRFCCPTCN0010 | ETAPA                   | 1                                                                        |     0 |        1 |
| RPRTTRFCCPTCN0010 | VAL_COLA_EJECUCION      | reportes                                                                 |     0 |        1 |
| RPRTTRFCCPTCN0010 | VAL_MASTER              | yarn                                                                     |     0 |        1 |
| RPRTTRFCCPTCN0010 | VAL_DRIVER_MEMORY       | 8G                                                                       |     0 |        1 |
| RPRTTRFCCPTCN0010 | VAL_EXECUTOR_MEMORY     | 8G                                                                       |     0 |        1 |
| RPRTTRFCCPTCN0010 | VAL_NUM_EXECUTORS       | 4                                                                        |     0 |        1 |
| RPRTTRFCCPTCN0010 | VAL_NUM_EXECUTORS_CORES | 4                                                                        |     0 |        1 |
+-------------------+-------------------------+--------------------------------------------------------------------------+-------+----------+

---PARAMETROS PARA LA ENTIDAD D_RPRTTRFCCPTCN0010
DELETE FROM params_des WHERE ENTIDAD = 'D_RPRTTRFCCPTCN0010';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','PARAM1_FECHA_EJECUCION',"date_format(sysdate(),'%Y%m%d')",'0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','SHELL','/home/nae108834/RGenerator/reportes/TRAFICO_CAPTACION/bin/SH_TRAFICO_CAPTACION.sh','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','VAL_RUTA','/home/nae108834/RGenerator/reportes/TRAFICO_CAPTACION','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','VAL_LOCAL_RUTA_OUT','/home/nae108834/RGenerator/reportes/TRAFICO_CAPTACION/output','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','VAL_NOM_FILE_OUT','d_captacion_pospago.csv','0','0'); --- CAMBIA NOMBRE ARCHIVO
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','VAL_ENCODING','ascii','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','VAL_SFTP_RUTA_OUT','"/Share/comun/gestion&proyectos/INFORMACION~}<EXTERNA/Captacion_Pos_Pre"','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','ESQUEMA','db_desarrollo2021','0','0');  --db_reportes
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','ESQUEMA_TMP','db_desarrollo2021','0','0'); --db_temporales
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','ETAPA','1','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','VAL_COLA_EJECUCION','capa_semantica','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','VAL_MASTER','yarn','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','VAL_DRIVER_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','VAL_EXECUTOR_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','VAL_NUM_EXECUTORS','8','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTTRFCCPTCN0010','VAL_NUM_EXECUTORS_CORES','8','0','0');
SELECT * FROM params_des WHERE ENTIDAD = "D_RPRTTRFCCPTCN0010";

