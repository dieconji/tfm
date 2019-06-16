from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext

conf = SparkConf()
conf.setMaster('yarn')
conf.setAppName('crear_tablon')

sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

sqlContext.sql('USE u257030')

sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'INICIO CREAR TABLON' operacion")
#
### CLIENTE + CONTRATO
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'CLIENTE + CONTRATO' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.cliente_df")
cliente_df = sqlContext.sql("SELECT distinct c.cod_cliente, ct.cod_contrato, c.can_antiguedad, c.cod_ayuntamiento, c.cod_calle, trim(c.cod_cnae_cli) as cod_cnae_cli, c.cod_ent_singular, c.cod_gestor_of, trim(c.cod_grupo_cliente) as cod_grupo_cliente, c.cod_idioma_dtf, trim(c.cod_moneda_dtf) as cod_moneda_dtf, c.cod_multis, c.cod_nivel_5, trim(c.cod_pais) as cod_pais, c.cod_pais_emi_dco_i, c.cod_provincia, trim(c.cod_segmento_1) as cod_segmento_1, trim(c.cod_segmento_2) as cod_segmento_2, trim(c.cod_segmento_3) as cod_segmento_3, trim(c.cod_segmento_4) as cod_segmento_4, trim(c.cod_segmento_5) as cod_segmento_5, trim(c.cod_tip_pfd_imprim) as cod_tip_pfd_imprim, trim(c.cod_tip_pfd_mensaj) as cod_tip_pfd_mensaj, c.cod_usu_verif, c.cod_version, c.desc_tipo_recobro, case when c.dir_email is null then 0 else 1 end as mail1, case when c.dir_email_2 is null then 0 else 1 end as mail2, case when c.num_telefono is null then 0 else 1 end as telefono, to_date(cast(c.fec_alta_cliente/1000 as TIMESTAMP)) as fec_alta_cliente, case when c.fec_baja_cliente=253370764800000 then null else to_date(cast(c.fec_baja_cliente/1000 as TIMESTAMP)) end as fec_baja_cliente, case when c.fec_nac_cte=253370764800000 then null else to_date(cast(c.fec_nac_cte/1000 as TIMESTAMP)) end as fec_nac_cte, case when c.fec_nac_cte=253370764800000 then null else cast(datediff(current_timestamp(), cast(c.fec_nac_cte/1000 as TIMESTAMP)) / 365 as INT) end as edad, to_date(cast(c.fec_verificacion/1000 as TIMESTAMP)) as fec_verificacion, c.ind_cli_vulnerable, c.ind_cliente_pot, c.ind_itl_gas, c.ind_ofc_vir_cli, c.num_hijos, trim(c.tip_clase_cli) as tip_clase_cli, trim(c.tip_cli_vulnerable) tip_cli_vulnerable, trim(c.tip_cliente) as tip_cliente, trim(c.tip_docmto_ide) as tip_docmto_ide, trim(c.tip_entidad_jur) as tip_entidad_jur, trim(c.tip_est_civil) as tip_est_civil, trim(c.tip_mercado_venta) as tip_mercado_venta, trim(c.tip_propen_abandono) as tip_propen_abandono, trim(c.tip_sexo) tip_sexo, trim(c.tip_sr_sra_empresa) as tip_sr_sra_empresa, trim(ct.tip_est_contrato) as tip_est_contrato, trim(ct.tip_emision_fca) as tip_emision_fca, trim(ct.tip_modo_pago) as tip_modo_pago, trim(ct.cod_tension) as cod_tension, trim(ct.cod_tension_med) as cod_tension_med, trim(ct.cod_tarifa_ibdla) as cod_tarifa_ibdla, trim(ct.tip_tarifa_boe) as tip_tarifa_boe, trim(ct.tip_vulnerable_bs) as tip_vulnerable_bs, trim(ct.ind_autogenerador) as ind_autogenerador, trim(ct.ind_bienergia) as ind_bienergia, ct.ind_cnd_apremi_cte, ct.ind_cnd_espec, ct.ind_cortante, ct.ind_cto_vulnerable, ct.ind_cuota_fija, ct.ind_deuda, ct.ind_domic, ct.ind_envio_propag, ct.ind_facturacion_eltrn, ct.ind_int_dmr, ct.ind_mserv, ct.ind_plan_person, ct.ind_servi_esencial, ct.ind_sur FROM dwvp.gtl_cliente c INNER JOIN dwvp.gtl_contrato ct ON c.cod_cliente=ct.cod_cliente")# where c.cod_cliente=17371322")
#cliente_df = cliente_df.sample(False, fraction=0.5).limit(1000000)
cliente_df.createOrReplaceTempView("TMP_CLIENTE_DF")
sqlContext.sql("CREATE TABLE u257030.cliente_df USING PARQUET AS SELECT * FROM TMP_CLIENTE_DF")
#
### FACTURAS
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'FACTURAS' operacion")
facturas_df = sqlContext.sql("SELECT cod_cliente, cod_contrato, avg(imp_total_factu) as importe_medio FROM dwvp.ghh_factura GROUP BY cod_cliente, cod_contrato")
sqlContext.sql("DROP TABLE IF EXISTS u257030.facturas_df")
facturas_df.createOrReplaceTempView("tmp_facturas_df")
sqlContext.sql("CREATE TABLE u257030.facturas_df USING PARQUET AS SELECT * FROM tmp_facturas_df")
#
### RECLAMACION
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'RECLAMACION' operacion")
reclamacion_df = sqlContext.sql("SELECT cod_cliente, cod_contrato, count(distinct num_reclamacion) as reclamaciones FROM dwvp.gth_reclamacion GROUP BY cod_cliente, cod_contrato")
sqlContext.sql("DROP TABLE IF EXISTS u257030.reclamacion_df")
reclamacion_df.createOrReplaceTempView("tmp_reclamacion_df")
sqlContext.sql("CREATE TABLE u257030.reclamacion_df USING PARQUET AS SELECT * FROM tmp_reclamacion_df")
#
### facturaelectronica
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'facturaelectronica' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.gestiones_facturaelectronica")
sql = sqlContext.sql("select cliente, contrato, count(distinct session_id) as facturaelectronica, min(fecha_evento) min_evento, max(fecha_evento) max_evento from u257030.mis_gestiones where url like '%facturaelectronica%' group by cliente, contrato")
sql.createOrReplaceTempView("tmp")
sqlContext.sql("CREATE TABLE u257030.gestiones_facturaelectronica USING PARQUET AS SELECT * FROM tmp")
#
### cuentabancaria
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'cuentabancaria' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.gestiones_cuentabancaria")
sql = sqlContext.sql("select cliente, contrato, count(distinct session_id) as cuentabancaria, min(fecha_evento) min_evento, max(fecha_evento) max_evento from u257030.mis_gestiones where url like '%cuentabancaria%' group by cliente, contrato")
sql.createOrReplaceTempView("tmp")
sqlContext.sql("CREATE TABLE u257030.gestiones_cuentabancaria USING PARQUET AS SELECT * FROM tmp")
#
### potencia
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'potencia' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.gestiones_potencia")
sql = sqlContext.sql("select cliente, contrato, count(distinct session_id) as potencia, min(fecha_evento) min_evento, max(fecha_evento) max_evento from u257030.mis_gestiones where url like '%potencia%' group by cliente, contrato")
sql.createOrReplaceTempView("tmp")
sqlContext.sql("CREATE TABLE u257030.gestiones_potencia USING PARQUET AS SELECT * FROM tmp")
#
### plan8Horas
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'plan8Horas' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.gestiones_plan8Horas")
sql = sqlContext.sql("select cliente, contrato, count(distinct session_id) as plan8Horas, min(fecha_evento) min_evento, max(fecha_evento) max_evento from u257030.mis_gestiones where url like '%plan8Horas%' group by cliente, contrato")
sql.createOrReplaceTempView("tmp")
sqlContext.sql("CREATE TABLE u257030.gestiones_plan8Horas USING PARQUET AS SELECT * FROM tmp")
#
### cuotafija
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'cuotafija' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.gestiones_cuotafija")
sql = sqlContext.sql("select cliente, contrato, count(distinct session_id) as cuotafija, min(fecha_evento) min_evento, max(fecha_evento) max_evento from u257030.mis_gestiones where url like '%cuotafija%' group by cliente, contrato")
sql.createOrReplaceTempView("tmp")
sqlContext.sql("CREATE TABLE u257030.gestiones_cuotafija USING PARQUET AS SELECT * FROM tmp")
#
### quejas
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'quejas' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.gestiones_quejas")
sql = sqlContext.sql("select cliente, contrato, count(distinct session_id) as quejas, min(fecha_evento) min_evento, max(fecha_evento) max_evento from u257030.mis_gestiones where url like '%quejas%' group by cliente, contrato")
sql.createOrReplaceTempView("tmp")
sqlContext.sql("CREATE TABLE u257030.gestiones_quejas USING PARQUET AS SELECT * FROM tmp")
#
### certificado
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'certificado' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.gestiones_certificado")
sql = sqlContext.sql("select cliente, contrato, count(distinct session_id) as certificado, min(fecha_evento) min_evento, max(fecha_evento) max_evento from u257030.mis_gestiones where url like '%certificado%' group by cliente, contrato")
sql.createOrReplaceTempView("tmp")
sqlContext.sql("CREATE TABLE u257030.gestiones_certificado USING PARQUET AS SELECT * FROM tmp")
#
### colaboracionAECC
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'colaboracionAECC' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.gestiones_colaboracionAECC")
sql = sqlContext.sql("select cliente, contrato, count(distinct session_id) as colaboracionAECC, min(fecha_evento) min_evento, max(fecha_evento) max_evento from u257030.mis_gestiones where url like '%colaboracionAECC%' group by cliente, contrato")
sql.createOrReplaceTempView("tmp")
sqlContext.sql("CREATE TABLE u257030.gestiones_colaboracionAECC USING PARQUET AS SELECT * FROM tmp")
#
### direccion
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'direccion' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.gestiones_direccion")
sql = sqlContext.sql("select cliente, contrato, count(distinct session_id) as direccion, min(fecha_evento) min_evento, max(fecha_evento) max_evento from u257030.mis_gestiones where url like '%direccion%' group by cliente, contrato")
sql.createOrReplaceTempView("tmp")
sqlContext.sql("CREATE TABLE u257030.gestiones_direccion USING PARQUET AS SELECT * FROM tmp")
#
### lecturaconsumo
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'lecturaconsumo' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.gestiones_lecturaconsumo")
sql = sqlContext.sql("select cliente, contrato, count(distinct session_id) as lecturaconsumo, min(fecha_evento) min_evento, max(fecha_evento) max_evento from u257030.mis_gestiones where url like '%lecturacontador%' group by cliente, contrato")
sql.createOrReplaceTempView("tmp")
sqlContext.sql("CREATE TABLE u257030.gestiones_lecturaconsumo USING PARQUET AS SELECT * FROM tmp")
#
### sesiones_total
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'sesiones_total' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.sesiones_total")
sql = sqlContext.sql("select cliente, contrato, sum(sesiones) sesiones_total from u257030.sesiones_x_cliente where year*10000+month*100+day<20190401 and sesiones>0 group by cliente, contrato")
sql.createOrReplaceTempView("tmp_sesiones_total")
sqlContext.sql("CREATE TABLE u257030.sesiones_total USING PARQUET AS SELECT * FROM tmp_sesiones_total")
#
### sesiones_ult_mes
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'sesiones_ult_mes' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.sesiones_ult_mes")
sql = sqlContext.sql("select cliente, contrato, sum(sesiones) sesiones_ult_mes from u257030.sesiones_x_cliente where year*10000+month*100+day>=20190301 and year*10000+month*100+day<20190401 and sesiones>0 group by cliente, contrato")
sql.createOrReplaceTempView("tmp_sesiones_ult_mes")
sqlContext.sql("CREATE TABLE u257030.sesiones_ult_mes USING PARQUET AS SELECT * FROM tmp_sesiones_ult_mes")
#
### sesiones_ult_anyo
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'sesiones_ult_anyo' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.sesiones_ult_anyo")
sql = sqlContext.sql("select cliente, contrato, sum(sesiones) sesiones_ult_anyo from u257030.sesiones_x_cliente where year*10000+month*100+day>=20180401 and year*10000+month*100+day<20190401 and sesiones>0 group by cliente, contrato")
sql.createOrReplaceTempView("tmp_sesiones_ult_anyo")
sqlContext.sql("CREATE TABLE u257030.sesiones_ult_anyo USING PARQUET AS SELECT * FROM tmp_sesiones_ult_anyo")
#
#### CLIENTE_ENR
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'CLIENTE_ENR' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.cliente_enr_df")
cliente_enr_df = sqlContext.sql("SELECT c.*, f.importe_medio, r.reclamaciones, st.sesiones_total, sm.sesiones_ult_mes, sa.sesiones_ult_anyo, g_facturaelectronica.facturaelectronica, to_date(cast(g_facturaelectronica.min_evento/1000 as TIMESTAMP)) as prim_facturaelectronica, to_date(cast(g_facturaelectronica.max_evento/1000 as TIMESTAMP)) as ult_facturaelectronica , g_cuentabancaria.cuentabancaria, to_date(cast(g_cuentabancaria.min_evento/1000 as TIMESTAMP)) as prim_cuentabancaria, to_date(cast(g_cuentabancaria.max_evento/1000 as TIMESTAMP)) as ult_cuentabancaria , g_potencia.potencia, to_date(cast(g_potencia.min_evento/1000 as TIMESTAMP)) as prim_potencia, to_date(cast(g_potencia.max_evento/1000 as TIMESTAMP)) as ult_potencia , g_plan8Horas.plan8Horas, to_date(cast(g_plan8Horas.min_evento/1000 as TIMESTAMP)) as prim_plan8Horas, to_date(cast(g_plan8Horas.max_evento/1000 as TIMESTAMP)) as ult_plan8Horas , g_cuotafija.cuotafija, to_date(cast(g_cuotafija.min_evento/1000 as TIMESTAMP)) as prim_cuotafija, to_date(cast(g_cuotafija.max_evento/1000 as TIMESTAMP)) as ult_cuotafija , g_quejas.quejas, to_date(cast(g_quejas.min_evento/1000 as TIMESTAMP)) as prim_quejas, to_date(cast(g_quejas.max_evento/1000 as TIMESTAMP)) as ult_quejas , g_certificado.certificado, to_date(cast(g_certificado.min_evento/1000 as TIMESTAMP)) as prim_certificado, to_date(cast(g_certificado.max_evento/1000 as TIMESTAMP)) as ult_certificado , g_colaboracionAECC.colaboracionAECC, to_date(cast(g_colaboracionAECC.min_evento/1000 as TIMESTAMP)) as prim_colaboracionAECC, to_date(cast(g_colaboracionAECC.max_evento/1000 as TIMESTAMP)) as ult_colaboracionAECC , g_direccion.direccion, to_date(cast(g_direccion.min_evento/1000 as TIMESTAMP)) as prim_direccion, to_date(cast(g_direccion.max_evento/1000 as TIMESTAMP)) as ult_direccion , g_lecturaconsumo.lecturaconsumo, to_date(cast(g_lecturaconsumo.min_evento/1000 as TIMESTAMP)) as prim_lecturaconsumo, to_date(cast(g_lecturaconsumo.max_evento/1000 as TIMESTAMP)) as ult_lecturaconsumo from u257030.cliente_df c LEFT JOIN u257030.facturas_df f ON c.cod_contrato=f.cod_contrato and c.cod_cliente=f.cod_cliente LEFT JOIN u257030.reclamacion_df r ON c.cod_contrato=r.cod_contrato and c.cod_cliente=r.cod_cliente LEFT JOIN u257030.sesiones_total st ON c.cod_contrato=st.contrato and c.cod_cliente=st.cliente LEFT JOIN u257030.sesiones_ult_anyo sa ON c.cod_contrato=sa.contrato and c.cod_cliente=sa.cliente LEFT JOIN sesiones_ult_mes sm ON c.cod_contrato=sm.contrato and c.cod_cliente=sm.cliente left join u257030.gestiones_facturaelectronica g_facturaelectronica on c.cod_cliente=g_facturaelectronica.cliente and c.cod_contrato=g_facturaelectronica.contrato left join u257030.gestiones_cuentabancaria g_cuentabancaria on c.cod_cliente=g_cuentabancaria.cliente and c.cod_contrato=g_cuentabancaria.contrato left join u257030.gestiones_potencia g_potencia on c.cod_cliente=g_potencia.cliente and c.cod_contrato=g_potencia.contrato left join u257030.gestiones_plan8Horas g_plan8Horas on c.cod_cliente=g_plan8Horas.cliente and c.cod_contrato=g_plan8Horas.contrato left join u257030.gestiones_cuotafija g_cuotafija on c.cod_cliente=g_cuotafija.cliente and c.cod_contrato=g_cuotafija.contrato left join u257030.gestiones_quejas g_quejas on c.cod_cliente=g_quejas.cliente and c.cod_contrato=g_quejas.contrato left join u257030.gestiones_certificado g_certificado on c.cod_cliente=g_certificado.cliente and c.cod_contrato=g_certificado.contrato left join u257030.gestiones_colaboracionAECC g_colaboracionAECC on c.cod_cliente=g_colaboracionAECC.cliente and c.cod_contrato=g_colaboracionAECC.contrato left join u257030.gestiones_direccion g_direccion on c.cod_cliente=g_direccion.cliente and c.cod_contrato=g_direccion.contrato left join u257030.gestiones_lecturaconsumo g_lecturaconsumo on c.cod_cliente=g_lecturaconsumo.cliente and c.cod_contrato=g_lecturaconsumo.contrato")
cliente_enr_df.createOrReplaceTempView("tmp_cliente_enr_df")
sqlContext.sql("CREATE TABLE u257030.cliente_enr_df USING PARQUET AS SELECT * FROM tmp_cliente_enr_df")
#
### ACCION_CLI
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'ACCION_CLI' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.ACCION_CLI_DF")
accion_cli_df = sqlContext.sql("SELECT a.des_comentario, a.cod_sociedad, a.cod_accion_cliente, trim(a.cod_interac_cli) as cod_interac_cli, trim(a.cod_usuario) as cod_usuario, trim(a.cod_cla_ent_adicio) as cod_cla_ent_adicio, trim(a.cod_subtip_acc_cli) as cod_subtip_acc_cli, case when trim(a.cod_subtip_acc_cli) in ('H3', 'B1', 'OW', 'AJ7', 'AJ8', 'AEU', 'AEV', 'VR', 'VT', '4G', '53', 'ABS', 'QM') then 'FacturaElectronica' when trim(a.cod_subtip_acc_cli) in ('BBN', 'AEE', 'AET', 'VK', 'X7', 'A8') then 'CambioCuenta' when trim(a.cod_subtip_acc_cli) in ('ACV', 'AEW', 'YI', 'YJ', '23') then 'CambioPotencia' when trim(a.cod_subtip_acc_cli) in ('GW', 'GX', 'ANW', 'ANZ', 'AOD', 'AOU', 'APJ', 'APM', 'APS', 'AT', 'BBD', 'BBQ', 'BQ', 'DF', 'AJA', 'AKC', 'AKD', 'AKH', 'AKY', 'ALM', 'ALP', 'ALV', 'AMS', '60', '8S', '9F', 'ADN', 'AEY', 'QR', 'QW') then 'QuejasReclamaciones' when trim(a.cod_subtip_acc_cli) in ('BS', '6L', 'AEK', 'AG6', 'XJ', 'AA7') then 'CertificadoE' when trim(a.cod_subtip_acc_cli) in ('AD6', 'AD7', 'AD8', 'AD9', 'ADA') then 'JuntosContraElCancer' else '' end as tipo, trim(a.cod_mot_acc_cli) as cod_mot_acc_cli, trim(a.cod_tip_acc_cli) as cod_tip_acc_cli, trim(a.cod_accion_usuario) as cod_accion_usuario, trim(a.cod_clave_entidad) as cod_clave_entidad, trim(a.cod_entidad) as cod_entidad, to_date(a.feh_fecha_alta) as feh_fecha_alta, to_date(a.feh_fecha_modif) as feh_fecha_modif, trim(a.tip_cer_accion) as tip_cer_accion, trim(a.cod_mdo_ala_acc) as cod_mdo_ala_acc, i.cod_ident_canal, trim(i.cod_llamada) cod_llamada, i.feh_fecha_hora, i.tip_canal FROM dwvp.gtl_accion_cliente_dia a INNER JOIN dwvp.gtl_interaccion_cliente_dia i ON trim(a.cod_interac_cli)=trim(i.cod_interac_cli) WHERE trim(cod_entidad)='T-CLIENTE'")
#accion_cli_df = accion_cli_df.sample(False, fraction=0.5).limit(500000)
accion_cli_df.createOrReplaceTempView("TMP_ACCION_CLI_DF")
sqlContext.sql("CREATE TABLE u257030.ACCION_CLI_DF USING PARQUET AS SELECT * FROM TMP_ACCION_CLI_DF")
#
### ACCION_CON
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'ACCION_CON' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.ACCION_CON_DF")
accion_con_df = sqlContext.sql("SELECT a.des_comentario, a.cod_sociedad, a.cod_accion_cliente, trim(a.cod_interac_cli) as cod_interac_cli, trim(a.cod_usuario) as cod_usuario, trim(a.cod_cla_ent_adicio) as cod_cla_ent_adicio, trim(a.cod_subtip_acc_cli) as cod_subtip_acc_cli, case when trim(a.cod_subtip_acc_cli) in ('H3', 'B1', 'OW', 'AJ7', 'AJ8', 'AEU', 'AEV', 'VR', 'VT', '4G', '53', 'ABS', 'QM') then 'FacturaElectronica' when trim(a.cod_subtip_acc_cli) in ('BBN', 'AEE', 'AET', 'VK', 'X7', 'A8') then 'CambioCuenta' when trim(a.cod_subtip_acc_cli) in ('ACV', 'AEW', 'YI', 'YJ', '23') then 'CambioPotencia' when trim(a.cod_subtip_acc_cli) in ('GW', 'GX', 'ANW', 'ANZ', 'AOD', 'AOU', 'APJ', 'APM', 'APS', 'AT', 'BBD', 'BBQ', 'BQ', 'DF', 'AJA', 'AKC', 'AKD', 'AKH', 'AKY', 'ALM', 'ALP', 'ALV', 'AMS', '60', '8S', '9F', 'ADN', 'AEY', 'QR', 'QW') then 'QuejasReclamaciones' when trim(a.cod_subtip_acc_cli) in ('BS', '6L', 'AEK', 'AG6', 'XJ', 'AA7') then 'CertificadoE' when trim(a.cod_subtip_acc_cli) in ('AD6', 'AD7', 'AD8', 'AD9', 'ADA') then 'JuntosContraElCancer' else '' end as tipo, trim(a.cod_mot_acc_cli) as cod_mot_acc_cli, trim(a.cod_tip_acc_cli) as cod_tip_acc_cli, trim(a.cod_accion_usuario) as cod_accion_usuario, trim(a.cod_clave_entidad) as cod_clave_entidad, trim(a.cod_entidad) as cod_entidad, to_date(a.feh_fecha_alta) as feh_fecha_alta, to_date(a.feh_fecha_modif) as feh_fecha_modif, trim(a.tip_cer_accion) as tip_cer_accion, trim(a.cod_mdo_ala_acc) as cod_mdo_ala_acc, i.cod_ident_canal, trim(i.cod_llamada) cod_llamada, i.feh_fecha_hora, i.tip_canal FROM dwvp.gtl_accion_cliente_dia a INNER JOIN dwvp.gtl_interaccion_cliente_dia i ON trim(a.cod_interac_cli)=trim(i.cod_interac_cli) WHERE trim(cod_entidad)='T-CONTRATO'")
#accion_con_df = accion_con_df.sample(False, fraction=0.5).limit(500000)
accion_con_df.createOrReplaceTempView("TMP_ACCION_CON_DF")
sqlContext.sql("CREATE TABLE u257030.ACCION_CON_DF USING PARQUET AS SELECT * FROM TMP_ACCION_CON_DF")
#
### ACCION_CLI_ENR
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'ACCION_CLI_ENR' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.accion_cli_enr_df")
accion_cli_enr_df = sqlContext.sql("SELECT a.*, c.* FROM u257030.accion_cli_df a inner join u257030.cliente_enr_df c on trim(a.cod_clave_entidad)=trim(c.cod_cliente)")
accion_cli_enr_df.createOrReplaceTempView("tmp_accion_cli_enr_df")
sqlContext.sql("CREATE TABLE u257030.accion_cli_enr_df USING PARQUET AS SELECT * FROM tmp_accion_cli_enr_df")
#
### ACCION_CON_ENR
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'ACCION_CON_ENR' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.accion_con_enr_df")
accion_con_enr_df = sqlContext.sql("SELECT a.*, c.* FROM u257030.accion_con_df a inner join u257030.cliente_enr_df c on trim(a.cod_clave_entidad)=trim(c.cod_contrato)")
accion_con_enr_df.createOrReplaceTempView("tmp_accion_con_enr_df")
sqlContext.sql("CREATE TABLE u257030.accion_con_enr_df USING PARQUET AS SELECT * FROM tmp_accion_con_enr_df")
#
### ACCION_ENR
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'ACCION_ENR' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.accion_enr_df")
accion_enr_df = sqlContext.sql("SELECT * FROM u257030.accion_cli_enr_df UNION SELECT * FROM u257030.accion_con_enr_df")
accion_enr_df.createOrReplaceTempView("tmp_accion_enr_df")
sqlContext.sql("CREATE TABLE u257030.accion_enr_df USING PARQUET AS SELECT * FROM tmp_accion_enr_df")
#
### ACCION CSV
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'ACCION CSV' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.accion_enr_df_csv")
sqlContext.sql("CREATE TABLE u257030.accion_enr_df_csv Row format delimited Fields terminated by '|' STORED AS TEXTFILE AS select 'cod_sociedad' as cod_sociedad, 'cod_accion_cliente' as cod_accion_cliente, 'cod_interac_cli' as cod_interac_cli, 'cod_usuario' as cod_usuario, 'cod_cla_ent_adicio' as cod_cla_ent_adicio, 'cod_subtip_acc_cli' as cod_subtip_acc_cli, 'tipo' as tipo, 'cod_mot_acc_cli' as cod_mot_acc_cli, 'cod_tip_acc_cli' as cod_tip_acc_cli, 'cod_accion_usuario' as cod_accion_usuario, 'cod_clave_entidad' as cod_clave_entidad, 'cod_entidad' as cod_entidad, 'feh_fecha_alta' as feh_fecha_alta, 'feh_fecha_modif' as feh_fecha_modif, 'tip_cer_accion' as tip_cer_accion, 'cod_mdo_ala_acc' as cod_mdo_ala_acc, 'cod_ident_canal' as cod_ident_canal, 'cod_llamada' as cod_llamada, 'feh_fecha_hora' as feh_fecha_hora, 'tip_canal' as tip_canal, 'cod_cliente' as cod_cliente, 'cod_contrato' as cod_contrato, 'can_antiguedad' as can_antiguedad, 'cod_idioma_dtf' as cod_idioma_dtf, 'cod_moneda_dtf' as cod_moneda_dtf, 'cod_multis' as cod_multis, 'cod_nivel_5' as cod_nivel_5, 'cod_pais' as cod_pais, 'cod_pais_emi_dco_i' as cod_pais_emi_dco_i, 'cod_provincia' as cod_provincia, 'desc_tipo_recobro' as desc_tipo_recobro, 'mail1' as mail1, 'mail2' as mail2, 'telefono' as telefono, 'fec_alta_cliente' as fec_alta_cliente, 'fec_baja_cliente' as fec_baja_cliente, 'fec_nac_cte' as fec_nac_cte, 'edad' as edad, 'fec_verificacion' as fec_verificacion, 'ind_cli_vulnerable' as ind_cli_vulnerable, 'ind_cliente_pot' as ind_cliente_pot, 'ind_itl_gas' as ind_itl_gas, 'ind_ofc_vir_cli' as ind_ofc_vir_cli, 'num_hijos' as num_hijos, 'tip_clase_cli' as tip_clase_cli, 'tip_cli_vulnerable' as tip_cli_vulnerable, 'tip_cliente' as tip_cliente, 'tip_docmto_ide' as tip_docmto_ide, 'tip_entidad_jur' as tip_entidad_jur, 'tip_est_civil' as tip_est_civil, 'tip_mercado_venta' as tip_mercado_venta, 'tip_propen_abandono' as tip_propen_abandono, 'tip_sexo' as tip_sexo, 'tip_sr_sra_empresa' as tip_sr_sra_empresa, 'tip_est_contrato' as tip_est_contrato, 'tip_emision_fca' as tip_emision_fca, 'tip_modo_pago' as tip_modo_pago, 'cod_tension' as cod_tension, 'cod_tension_med' as cod_tension_med, 'cod_tarifa_ibdla' as cod_tarifa_ibdla, 'tip_tarifa_boe' as tip_tarifa_boe, 'tip_vulnerable_bs' as tip_vulnerable_bs, 'ind_autogenerador' as ind_autogenerador, 'ind_bienergia' as ind_bienergia, 'ind_cnd_apremi_cte' as ind_cnd_apremi_cte, 'ind_cnd_espec' as ind_cnd_espec, 'ind_cortante' as ind_cortante, 'ind_cto_vulnerable' as ind_cto_vulnerable, 'ind_cuota_fija' as ind_cuota_fija, 'ind_deuda' as ind_deuda, 'ind_domic' as ind_domic, 'ind_envio_propag' as ind_envio_propag, 'ind_facturacion_eltrn' as ind_facturacion_eltrn, 'ind_int_dmr' as ind_int_dmr, 'ind_mserv' as ind_mserv, 'ind_plan_person' as ind_plan_person, 'ind_servi_esencial' as ind_servi_esencial, 'ind_sur' as ind_sur, 'importe_medio' as importe_medio, 'reclamaciones' as reclamaciones, 'sesiones_total' as sesiones_total, 'sesiones_ult_mes' as sesiones_ult_mes, 'sesiones_ult_anyo' as sesiones_ult_anyo, 'facturaelectronica' as facturaelectronica, 'prim_facturaelectronica' as prim_facturaelectronica, 'ult_facturaelectronica' as ult_facturaelectronica, 'cuentabancaria' as cuentabancaria, 'prim_cuentabancaria' as prim_cuentabancaria, 'ult_cuentabancaria' as ult_cuentabancaria, 'potencia' as potencia, 'prim_potencia' as prim_potencia, 'ult_potencia' as ult_potencia, 'plan8horas' as plan8horas, 'prim_plan8horas' as prim_plan8horas, 'ult_plan8horas' as ult_plan8horas, 'cuotafija' as cuotafija, 'prim_cuotafija' as prim_cuotafija, 'ult_cuotafija' as ult_cuotafija, 'quejas' as quejas, 'prim_quejas' as prim_quejas, 'ult_quejas' as ult_quejas, 'certificado' as certificado, 'prim_certificado' as prim_certificado, 'ult_certificado' as ult_certificado, 'colaboracionaecc' as colaboracionaecc, 'prim_colaboracionaecc' as prim_colaboracionaecc, 'ult_colaboracionaecc' as ult_colaboracionaecc, 'direccion' as direccion, 'prim_direccion' as prim_direccion, 'ult_direccion' as ult_direccion, 'lecturaconsumo' as lecturaconsumo, 'prim_lecturaconsumo' as prim_lecturaconsumo, 'ult_lecturaconsumo' as ult_lecturaconsumo")
sqlContext.sql("insert into u257030.accion_enr_df_csv select cod_sociedad, cod_accion_cliente, cod_interac_cli, cod_usuario, cod_cla_ent_adicio, cod_subtip_acc_cli, tipo, cod_mot_acc_cli, cod_tip_acc_cli, cod_accion_usuario, cod_clave_entidad, cod_entidad, feh_fecha_alta, feh_fecha_modif, tip_cer_accion, cod_mdo_ala_acc, cod_ident_canal, cod_llamada, feh_fecha_hora, tip_canal, cod_cliente, cod_contrato, can_antiguedad, cod_idioma_dtf, cod_moneda_dtf, cod_multis, cod_nivel_5, cod_pais, cod_pais_emi_dco_i, cod_provincia, desc_tipo_recobro, mail1, mail2, telefono, fec_alta_cliente, fec_baja_cliente, fec_nac_cte, edad, fec_verificacion, ind_cli_vulnerable, ind_cliente_pot, ind_itl_gas, ind_ofc_vir_cli, num_hijos, tip_clase_cli, tip_cli_vulnerable, tip_cliente, tip_docmto_ide, tip_entidad_jur, tip_est_civil, tip_mercado_venta, tip_propen_abandono, tip_sexo, tip_sr_sra_empresa, tip_est_contrato, tip_emision_fca, tip_modo_pago, cod_tension, cod_tension_med, cod_tarifa_ibdla, tip_tarifa_boe, tip_vulnerable_bs, ind_autogenerador, ind_bienergia, ind_cnd_apremi_cte, ind_cnd_espec, ind_cortante, ind_cto_vulnerable, ind_cuota_fija, ind_deuda, ind_domic, ind_envio_propag, ind_facturacion_eltrn, ind_int_dmr, ind_mserv, ind_plan_person, ind_servi_esencial, ind_sur, importe_medio, reclamaciones, sesiones_total, sesiones_ult_mes, sesiones_ult_anyo, facturaelectronica, prim_facturaelectronica, ult_facturaelectronica, cuentabancaria, prim_cuentabancaria, ult_cuentabancaria, potencia, prim_potencia, ult_potencia, plan8horas, prim_plan8horas, ult_plan8horas, cuotafija, prim_cuotafija, ult_cuotafija, quejas, prim_quejas, ult_quejas, certificado, prim_certificado, ult_certificado, colaboracionaecc, prim_colaboracionaecc, ult_colaboracionaecc, direccion, prim_direccion, ult_direccion, lecturaconsumo, prim_lecturaconsumo, ult_lecturaconsumo from u257030.accion_enr_df where tipo<>'' limit 1000000")
#
### ACCION_GROUP
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'ACCION_GROUP' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.accion_grp_df")
accion_grp_df = sqlContext.sql("select cod_cliente, cod_contrato, tipo, tip_canal, count(*) as total from u257030.accion_enr_df where tipo<>'' group by cod_cliente, cod_contrato, tipo, tip_canal")
accion_grp_df.createOrReplaceTempView("tmp_accion_grp_df")
sqlContext.sql("CREATE TABLE u257030.accion_grp_df USING PARQUET AS SELECT * FROM tmp_accion_grp_df")
#
### CLIENTE_TABLON
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'CLIENTE_TABLON' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.cliente_tbl_df")
cliente_tbl_df = sqlContext.sql("SELECT c.*, nvl(fe_w.total,0) as FacturaElectronica_Web, nvl(fe_o.total,0) as FacturaElectronica_Otros, nvl(fe_w.total,0) + nvl(fe_o.total,0) FacturaElectronica_Total , nvl(jc_w.total,0) as JuntosContraElCancer_Web, nvl(jc_o.total,0) as JuntosContraElCancer_Otros, nvl(jc_w.total,0) + nvl(jc_o.total,0) JuntosContraElCancer_Total , nvl(cc_w.total,0) as CambioCuenta_Web, nvl(cc_o.total,0) as CambioCuenta_Otros, nvl(cc_w.total,0) + nvl(cc_o.total,0) CambioCuenta_Total , nvl(qr_w.total,0) as QuejasReclamaciones_Web, nvl(qr_o.total,0) as QuejasReclamaciones_Otros, nvl(qr_w.total,0) + nvl(qr_o.total,0) QuejasReclamaciones_Total , nvl(cp_w.total,0) as CambioPotencia_Web, nvl(cp_o.total,0) as CambioPotencia_Otros, nvl(cp_w.total,0) + nvl(cp_o.total,0) CambioPotencia_Total , nvl(ce_w.total,0) as CertificadoE_Web, nvl(ce_o.total,0) as CertificadoE_Otros, nvl(ce_w.total,0) + nvl(ce_o.total,0) CertificadoE_Total FROM u257030.cliente_enr_df as c LEFT JOIN u257030.accion_grp_df as fe_w ON c.cod_cliente=fe_w.cod_cliente and c.cod_contrato=fe_w.cod_contrato and fe_w.tipo='FacturaElectronica' and fe_w.tip_canal='WE' LEFT JOIN u257030.accion_grp_df as fe_o ON c.cod_cliente=fe_o.cod_cliente and c.cod_contrato=fe_o.cod_contrato and fe_o.tipo='FacturaElectronica' and fe_o.tip_canal<>'WE' LEFT JOIN u257030.accion_grp_df as jc_w ON c.cod_cliente=jc_w.cod_cliente and c.cod_contrato=jc_w.cod_contrato and jc_w.tipo='JuntosContraElCancer' and jc_w.tip_canal='WE' LEFT JOIN u257030.accion_grp_df as jc_o ON c.cod_cliente=jc_o.cod_cliente and c.cod_contrato=jc_o.cod_contrato and jc_o.tipo='JuntosContraElCancer' and jc_o.tip_canal<>'WE' LEFT JOIN u257030.accion_grp_df as cc_w ON c.cod_cliente=cc_w.cod_cliente and c.cod_contrato=cc_w.cod_contrato and cc_w.tipo='CambioCuenta' and cc_w.tip_canal='WE' LEFT JOIN u257030.accion_grp_df as cc_o ON c.cod_cliente=cc_o.cod_cliente and c.cod_contrato=cc_o.cod_contrato and cc_o.tipo='CambioCuenta' and cc_o.tip_canal<>'WE' LEFT JOIN u257030.accion_grp_df as qr_w ON c.cod_cliente=qr_w.cod_cliente and c.cod_contrato=qr_w.cod_contrato and qr_w.tipo='QuejasReclamaciones' and qr_w.tip_canal='WE' LEFT JOIN u257030.accion_grp_df as qr_o ON c.cod_cliente=qr_o.cod_cliente and c.cod_contrato=qr_o.cod_contrato and qr_o.tipo='QuejasReclamaciones' and qr_o.tip_canal<>'WE' LEFT JOIN u257030.accion_grp_df as cp_w ON c.cod_cliente=cp_w.cod_cliente and c.cod_contrato=cp_w.cod_contrato and cp_w.tipo='CambioPotencia' and cp_w.tip_canal='WE' LEFT JOIN u257030.accion_grp_df as cp_o ON c.cod_cliente=cp_o.cod_cliente and c.cod_contrato=cp_o.cod_contrato and cp_o.tipo='CambioPotencia' and cp_o.tip_canal<>'WE' LEFT JOIN u257030.accion_grp_df as ce_w ON c.cod_cliente=ce_w.cod_cliente and c.cod_contrato=ce_w.cod_contrato and ce_w.tipo='CertificadoE' and ce_w.tip_canal='WE' LEFT JOIN u257030.accion_grp_df as ce_o ON c.cod_cliente=ce_o.cod_cliente and c.cod_contrato=ce_o.cod_contrato and ce_o.tipo='CertificadoE' and ce_o.tip_canal<>'WE'")
cliente_tbl_df.createOrReplaceTempView("tmp_cliente_tbl_df")
sqlContext.sql("CREATE TABLE u257030.cliente_tbl_df USING PARQUET AS SELECT * FROM tmp_cliente_tbl_df")
#
### CLIENTE CSV
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'CLIENTE CSV' operacion")
sqlContext.sql("DROP TABLE IF EXISTS u257030.cliente_tbl_df_csv")
sqlContext.sql("CREATE TABLE u257030.cliente_tbl_df_csv Row format delimited Fields terminated by '|' STORED AS TEXTFILE AS select 'cod_cliente' as cod_cliente, 'cod_contrato' as cod_contrato, 'can_antiguedad' as can_antiguedad, 'cod_idioma_dtf' as cod_idioma_dtf, 'cod_moneda_dtf' as cod_moneda_dtf, 'cod_multis' as cod_multis, 'cod_nivel_5' as cod_nivel_5, 'cod_pais' as cod_pais, 'cod_pais_emi_dco_i' as cod_pais_emi_dco_i, 'cod_provincia' as cod_provincia, 'desc_tipo_recobro' as desc_tipo_recobro, 'mail1' as mail1, 'mail2' as mail2, 'telefono' as telefono, 'fec_alta_cliente' as fec_alta_cliente, 'fec_baja_cliente' as fec_baja_cliente, 'fec_nac_cte' as fec_nac_cte, 'edad' as edad, 'fec_verificacion' as fec_verificacion, 'ind_cli_vulnerable' as ind_cli_vulnerable, 'ind_cliente_pot' as ind_cliente_pot, 'ind_itl_gas' as ind_itl_gas, 'ind_ofc_vir_cli' as ind_ofc_vir_cli, 'num_hijos' as num_hijos, 'tip_clase_cli' as tip_clase_cli, 'tip_cli_vulnerable' as tip_cli_vulnerable, 'tip_cliente' as tip_cliente, 'tip_docmto_ide' as tip_docmto_ide, 'tip_entidad_jur' as tip_entidad_jur, 'tip_est_civil' as tip_est_civil, 'tip_mercado_venta' as tip_mercado_venta, 'tip_propen_abandono' as tip_propen_abandono, 'tip_sexo' as tip_sexo, 'tip_sr_sra_empresa' as tip_sr_sra_empresa, 'tip_est_contrato' as tip_est_contrato, 'tip_emision_fca' as tip_emision_fca, 'tip_modo_pago' as tip_modo_pago, 'cod_tension' as cod_tension, 'cod_tension_med' as cod_tension_med, 'cod_tarifa_ibdla' as cod_tarifa_ibdla, 'tip_tarifa_boe' as tip_tarifa_boe, 'tip_vulnerable_bs' as tip_vulnerable_bs, 'ind_autogenerador' as ind_autogenerador, 'ind_bienergia' as ind_bienergia, 'ind_cnd_apremi_cte' as ind_cnd_apremi_cte, 'ind_cnd_espec' as ind_cnd_espec, 'ind_cortante' as ind_cortante, 'ind_cto_vulnerable' as ind_cto_vulnerable, 'ind_cuota_fija' as ind_cuota_fija, 'ind_deuda' as ind_deuda, 'ind_domic' as ind_domic, 'ind_envio_propag' as ind_envio_propag, 'ind_facturacion_eltrn' as ind_facturacion_eltrn, 'ind_int_dmr' as ind_int_dmr, 'ind_mserv' as ind_mserv, 'ind_plan_person' as ind_plan_person, 'ind_servi_esencial' as ind_servi_esencial, 'ind_sur' as ind_sur, 'importe_medio' as importe_medio, 'reclamaciones' as reclamaciones, 'sesiones_total' as sesiones_total, 'sesiones_ult_mes' as sesiones_ult_mes, 'sesiones_ult_anyo' as sesiones_ult_anyo, 'facturaelectronica' as facturaelectronica, 'prim_facturaelectronica' as prim_facturaelectronica, 'ult_facturaelectronica' as ult_facturaelectronica, 'cuentabancaria' as cuentabancaria, 'prim_cuentabancaria' as prim_cuentabancaria, 'ult_cuentabancaria' as ult_cuentabancaria, 'potencia' as potencia, 'prim_potencia' as prim_potencia, 'ult_potencia' as ult_potencia, 'plan8horas' as plan8horas, 'prim_plan8horas' as prim_plan8horas, 'ult_plan8horas' as ult_plan8horas, 'cuotafija' as cuotafija, 'prim_cuotafija' as prim_cuotafija, 'ult_cuotafija' as ult_cuotafija, 'quejas' as quejas, 'prim_quejas' as prim_quejas, 'ult_quejas' as ult_quejas, 'certificado' as certificado, 'prim_certificado' as prim_certificado, 'ult_certificado' as ult_certificado, 'colaboracionaecc' as colaboracionaecc, 'prim_colaboracionaecc' as prim_colaboracionaecc, 'ult_colaboracionaecc' as ult_colaboracionaecc, 'direccion' as direccion, 'prim_direccion' as prim_direccion, 'ult_direccion' as ult_direccion, 'lecturaconsumo' as lecturaconsumo, 'prim_lecturaconsumo' as prim_lecturaconsumo, 'ult_lecturaconsumo' as ult_lecturaconsumo, 'facturaelectronica_web' as facturaelectronica_web, 'facturaelectronica_otros' as facturaelectronica_otros, 'facturaelectronica_total' as facturaelectronica_total, 'juntoscontraelcancer_web' as juntoscontraelcancer_web, 'juntoscontraelcancer_otros' as juntoscontraelcancer_otros, 'juntoscontraelcancer_total' as juntoscontraelcancer_total, 'cambiocuenta_web' as cambiocuenta_web, 'cambiocuenta_otros' as cambiocuenta_otros, 'cambiocuenta_total' as cambiocuenta_total, 'quejasreclamaciones_web' as quejasreclamaciones_web, 'quejasreclamaciones_otros' as quejasreclamaciones_otros, 'quejasreclamaciones_total' as quejasreclamaciones_total, 'cambiopotencia_web' as cambiopotencia_web, 'cambiopotencia_otros' as cambiopotencia_otros, 'cambiopotencia_total' as cambiopotencia_total, 'certificadoe_web' as certificadoe_web, 'certificadoe_otros' as certificadoe_otros, 'certificadoe_total' as certificadoe_total")
sqlContext.sql("insert into u257030.cliente_tbl_df_csv select cod_cliente, cod_contrato, can_antiguedad, cod_idioma_dtf, cod_moneda_dtf, cod_multis, cod_nivel_5, cod_pais, cod_pais_emi_dco_i, cod_provincia, desc_tipo_recobro, mail1, mail2, telefono, fec_alta_cliente, fec_baja_cliente, fec_nac_cte, edad, fec_verificacion, ind_cli_vulnerable, ind_cliente_pot, ind_itl_gas, ind_ofc_vir_cli, num_hijos, tip_clase_cli, tip_cli_vulnerable, tip_cliente, tip_docmto_ide, tip_entidad_jur, tip_est_civil, tip_mercado_venta, tip_propen_abandono, tip_sexo, tip_sr_sra_empresa, tip_est_contrato, tip_emision_fca, tip_modo_pago, cod_tension, cod_tension_med, cod_tarifa_ibdla, tip_tarifa_boe, tip_vulnerable_bs, ind_autogenerador, ind_bienergia, ind_cnd_apremi_cte, ind_cnd_espec, ind_cortante, ind_cto_vulnerable, ind_cuota_fija, ind_deuda, ind_domic, ind_envio_propag, ind_facturacion_eltrn, ind_int_dmr, ind_mserv, ind_plan_person, ind_servi_esencial, ind_sur, importe_medio, reclamaciones, sesiones_total, sesiones_ult_mes, sesiones_ult_anyo, facturaelectronica, prim_facturaelectronica, ult_facturaelectronica, cuentabancaria, prim_cuentabancaria, ult_cuentabancaria, potencia, prim_potencia, ult_potencia, plan8horas, prim_plan8horas, ult_plan8horas, cuotafija, prim_cuotafija, ult_cuotafija, quejas, prim_quejas, ult_quejas, certificado, prim_certificado, ult_certificado, colaboracionaecc, prim_colaboracionaecc, ult_colaboracionaecc, direccion, prim_direccion, ult_direccion, lecturaconsumo, prim_lecturaconsumo, ult_lecturaconsumo, facturaelectronica_web, facturaelectronica_otros, facturaelectronica_total, juntoscontraelcancer_web, juntoscontraelcancer_otros, juntoscontraelcancer_total, cambiocuenta_web, cambiocuenta_otros, cambiocuenta_total, quejasreclamaciones_web, quejasreclamaciones_otros, quejasreclamaciones_total, cambiopotencia_web, cambiopotencia_otros, cambiopotencia_total, certificadoe_web, certificadoe_otros, certificadoe_total from u257030.cliente_tbl_df limit 1000000")
#
#
sqlContext.sql("insert into u257030.log select current_timestamp() fecha , 'FIN CREAR TABLON' operacion")



### copiar a local: hadoop fs -cat /hive/warehouse/u257030.db/accion_enr_df_csv/part-* > accion_enr_df_limit.csv
### hdfs dfs -cat /hive/warehouse/u257030.db/cliente_tbl_df_csv/part-* > cliente_tbl_df_limit.csv
