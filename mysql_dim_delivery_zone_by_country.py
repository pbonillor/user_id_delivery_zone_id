# se importan las librerias argparse y ConfigParser para recibir parametros en las funciones
import argparse, ConfigParser
# se importan las librerias os y sys para enviar comandos al sistema operativo.
import os, sys
# se importa la libreria traceback para manejo de errores y excepciones
import traceback
# se importa la libreria time para realiza un sleep mientras que spark finaliza la consolidacion de los archivos
import time
# Se importa la libreria pyspark las funciones para declarar el contexto (SparkContext) y definir la configuracion (SparkConf)
from pyspark import SparkContext, SparkConf
# Se importa la libreria pyspark.sql la funciona para definir el contexto de una consulta (SQLContext)
from pyspark.sql import SQLContext
# Se importa la libreria pyspark.sql.types todas las funciones de los tipos de datos en las consultas
from pyspark.sql.types import *
def funcion_dim_address(app_args):
        try:
                # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark, le pasa como parametro el pais
                conf = SparkConf().setAppName("mysql_dim_delivery_zone_by_"+app_args.pais+"_to_hdfs")
                # se crea la variable sc que defibne el contexto de ejecucion con el parametro de configuracion
                sc = SparkContext(conf=conf)
                # se crea la variable sqlContext para realizar la consulta
                sqlContext = SQLContext(sc)
                # se realiza la conexion a ODS sobre la tabla delivery_zone y se asigna a dataframe_mysql_delivery_zone
                dataframe_mysql_delivery_zone = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://10.0.92.65/").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "ods_partners.delivery_zones").option("user", "bigdata.etl").option("password", "KGoWI6nb4PRE9nd").option("useSSL", "false").load()
                # con el dataframe_mysql_delivery_zone se crea la tabla temporal de spark delivery_zone
                dataframe_mysql_delivery_zone.createOrReplaceTempView("delivery_zone")
                # se realiza la conexion a peyabi sobre la tabla dim_area y se asigna a dataframe_postgresql_dim_restaurant
                dataframe_postgresql_dim_restaurant = sqlContext.read.format("jdbc").option("url", "jdbc:postgresql://peyabi.czurab4ej1gp.us-east-1.redshift.amazonaws.com:5439/peyabi").option("dbtable", "public.dim_restaurant").option("user", "bigdata_etl").option("password", "zej4uoI9YR2m0Nxh84wi").option("useSSL", "false").load()
                # con el dataframe_mysql_restaurant se crea la tabla temporal de spark restaurant
                dataframe_postgresql_dim_restaurant.createOrReplaceTempView("restaurant")
                # se realiza la consulta del join para obtener el restaurant_id, delivery_zone_id y polygon de un pais especifico
                if app_args.pais == 'uy':
                        sqlDF_address_by_country = sqlContext.sql("SELECT restaurant.restaurant_id,restaurant.restaurant_name,restaurant.restaurant_state_id,restaurant.restaurant_type_id,restaurant.area_id,restaurant.country_id,restaurant.registered_date_id,restaurant.backend_id,restaurant.is_vip,restaurant.is_gold_vip,restaurant.is_important_account,restaurant.registered_date,restaurant.created_date_id,restaurant.created_date,restaurant.last_order_date,restaurant.first_order_date,restaurant.rut,restaurant.has_online_payment,restaurant.has_stamps,restaurant.has_pos,restaurant.restaurant_street,restaurant.restaurant_door_number,restaurant.restaurant_phone,restaurant.restaurant_owner_contact_name,restaurant.restaurant_owner_contact_last_name,restaurant.restaurant_owner_contact_phones,restaurant.reception_system_name,restaurant.commission,restaurant.max_shipping_amount,restaurant.min_delivery_amount as restaurant_min_delivery_amount,restaurant.publicity_cost,restaurant.automation_cost,restaurant.qty_orders,restaurant.qty_orders_confirmed,restaurant.qty_orders_rejected,restaurant.qty_orders_canceled,restaurant.qty_users,restaurant.total_amount_confirmed,restaurant.basket_size,restaurant.activity_rate,restaurant.avg_rating,restaurant.cancelation_rate,restaurant.failure_rate,restaurant.main_cousine,restaurant.qty_comments,restaurant.is_talent,restaurant.has_banner,restaurant.has_discount,restaurant.delivery_time,restaurant.max_delivery_amount,restaurant.min_shipping_amount,restaurant.qty_zones,restaurant.visits,restaurant.cvr,restaurant.acquisitions,restaurant.dt_1_15_y_30,restaurant.dt_2_30_y_45,restaurant.dt_3_45_y_60,restaurant.dt_4_60_y_90,restaurant.dt_5_90_y_120,restaurant.dt_6_24_horas,restaurant.dt_7_48_horas,restaurant.dt_8_72_horas,restaurant.dt_9_120_y_150,restaurant.dt_10_12_horas,restaurant.dt_11_150_y_180,restaurant.perc_orders_with_time_change,restaurant.qty_orders_without_time_change,restaurant.qty_orders_with_time_change,restaurant.dt_without,restaurant.is_express,restaurant.is_debtor,restaurant.contract_number,restaurant.has_mov,restaurant.qty_current_gold_vip,restaurant.has_featured_product,restaurant.qty_current_featured_product,restaurant.is_logistic,restaurant.account_owner,restaurant.address_id,restaurant.avg_speed,restaurant.is_premium,restaurant.not_use,restaurant.main_cousine_category_id,restaurant.previous_state_id,restaurant.change_state_date_id,restaurant.avg_qty_order_last_3_month,restaurant.avg_total_amount_last_3_month,restaurant.link,restaurant.logo,restaurant.reception_system_enabled,restaurant.is_day,restaurant.is_night,restaurant.city_id,restaurant.disabled_reason,restaurant.avg_food,restaurant.avg_service,restaurant.has_custom_photo_menu,restaurant.disabled_motive,restaurant.has_shipping_amount,restaurant.orders_reception_system_id,restaurant.orders_secondary_reception_system_id,restaurant.logistics_commission,restaurant.has_restaurant_portal,restaurant.dispatch_current_month,restaurant.sap_id,restaurant.commission_restaurant,restaurant.first_date_online,restaurant.delivery_type,restaurant.email,restaurant.business_type,restaurant.business_name,restaurant.billing_info_id,restaurant.is_online,restaurant.is_new_online,restaurant.is_offline,restaurant.accepts_vouchers,restaurant.is_active,restaurant.is_pending,restaurant.is_new_online_logistic,restaurant.is_chain,restaurant.is_new_registered,restaurant.url_site,restaurant.shipping_amount as restaurant_shipping_amount,restaurant.shipping_amount_is_percentage as restaurant_shipping_amount_is_percentage,restaurant.audi_load_date,delivery_zone.id as delivery_zone_id,delivery_zone.extra_time,delivery_zone.from_date,delivery_zone.is_deleted,delivery_zone.min_delivery_amount as delivery_zone_min_delivery_amount,delivery_zone.name,delivery_zone.polygon as polygon,delivery_zone.partner_id as delivery_zone_partner_id ,delivery_zone.shipping_amount as delivery_zone_shipping_amount ,delivery_zone.shipping_amount_is_percentage as delivery_zone_shipping_amount_is_percentage ,delivery_zone.surface,delivery_zone.to_date,delivery_zone.date_created,delivery_zone.last_updated,delivery_zone.delivery_time_id FROM delivery_zone JOIN restaurant on delivery_zone.partner_id = restaurant.restaurant_id where restaurant.country_id = 1")
                if app_args.pais == 'cl':
                        sqlDF_address_by_country = sqlContext.sql("SELECT restaurant.restaurant_id,restaurant.restaurant_name,restaurant.restaurant_state_id,restaurant.restaurant_type_id,restaurant.area_id,restaurant.country_id,restaurant.registered_date_id,restaurant.backend_id,restaurant.is_vip,restaurant.is_gold_vip,restaurant.is_important_account,restaurant.registered_date,restaurant.created_date_id,restaurant.created_date,restaurant.last_order_date,restaurant.first_order_date,restaurant.rut,restaurant.has_online_payment,restaurant.has_stamps,restaurant.has_pos,restaurant.restaurant_street,restaurant.restaurant_door_number,restaurant.restaurant_phone,restaurant.restaurant_owner_contact_name,restaurant.restaurant_owner_contact_last_name,restaurant.restaurant_owner_contact_phones,restaurant.reception_system_name,restaurant.commission,restaurant.max_shipping_amount,restaurant.min_delivery_amount as restaurant_min_delivery_amount,restaurant.publicity_cost,restaurant.automation_cost,restaurant.qty_orders,restaurant.qty_orders_confirmed,restaurant.qty_orders_rejected,restaurant.qty_orders_canceled,restaurant.qty_users,restaurant.total_amount_confirmed,restaurant.basket_size,restaurant.activity_rate,restaurant.avg_rating,restaurant.cancelation_rate,restaurant.failure_rate,restaurant.main_cousine,restaurant.qty_comments,restaurant.is_talent,restaurant.has_banner,restaurant.has_discount,restaurant.delivery_time,restaurant.max_delivery_amount,restaurant.min_shipping_amount,restaurant.qty_zones,restaurant.visits,restaurant.cvr,restaurant.acquisitions,restaurant.dt_1_15_y_30,restaurant.dt_2_30_y_45,restaurant.dt_3_45_y_60,restaurant.dt_4_60_y_90,restaurant.dt_5_90_y_120,restaurant.dt_6_24_horas,restaurant.dt_7_48_horas,restaurant.dt_8_72_horas,restaurant.dt_9_120_y_150,restaurant.dt_10_12_horas,restaurant.dt_11_150_y_180,restaurant.perc_orders_with_time_change,restaurant.qty_orders_without_time_change,restaurant.qty_orders_with_time_change,restaurant.dt_without,restaurant.is_express,restaurant.is_debtor,restaurant.contract_number,restaurant.has_mov,restaurant.qty_current_gold_vip,restaurant.has_featured_product,restaurant.qty_current_featured_product,restaurant.is_logistic,restaurant.account_owner,restaurant.address_id,restaurant.avg_speed,restaurant.is_premium,restaurant.not_use,restaurant.main_cousine_category_id,restaurant.previous_state_id,restaurant.change_state_date_id,restaurant.avg_qty_order_last_3_month,restaurant.avg_total_amount_last_3_month,restaurant.link,restaurant.logo,restaurant.reception_system_enabled,restaurant.is_day,restaurant.is_night,restaurant.city_id,restaurant.disabled_reason,restaurant.avg_food,restaurant.avg_service,restaurant.has_custom_photo_menu,restaurant.disabled_motive,restaurant.has_shipping_amount,restaurant.orders_reception_system_id,restaurant.orders_secondary_reception_system_id,restaurant.logistics_commission,restaurant.has_restaurant_portal,restaurant.dispatch_current_month,restaurant.sap_id,restaurant.commission_restaurant,restaurant.first_date_online,restaurant.delivery_type,restaurant.email,restaurant.business_type,restaurant.business_name,restaurant.billing_info_id,restaurant.is_online,restaurant.is_new_online,restaurant.is_offline,restaurant.accepts_vouchers,restaurant.is_active,restaurant.is_pending,restaurant.is_new_online_logistic,restaurant.is_chain,restaurant.is_new_registered,restaurant.url_site,restaurant.shipping_amount as restaurant_shipping_amount,restaurant.shipping_amount_is_percentage as restaurant_shipping_amount_is_percentage,restaurant.audi_load_date,delivery_zone.id as delivery_zone_id,delivery_zone.extra_time,delivery_zone.from_date,delivery_zone.is_deleted,delivery_zone.min_delivery_amount as delivery_zone_min_delivery_amount,delivery_zone.name,delivery_zone.polygon as polygon,delivery_zone.partner_id as delivery_zone_partner_id ,delivery_zone.shipping_amount as delivery_zone_shipping_amount ,delivery_zone.shipping_amount_is_percentage as delivery_zone_shipping_amount_is_percentage ,delivery_zone.surface,delivery_zone.to_date,delivery_zone.date_created,delivery_zone.last_updated,delivery_zone.delivery_time_id FROM delivery_zone JOIN restaurant on delivery_zone.partner_id = restaurant.restaurant_id where restaurant.country_id = 2")
                if app_args.pais == 'ar':
                        sqlDF_address_by_country = sqlContext.sql("SELECT restaurant.restaurant_id,restaurant.restaurant_name,restaurant.restaurant_state_id,restaurant.restaurant_type_id,restaurant.area_id,restaurant.country_id,restaurant.registered_date_id,restaurant.backend_id,restaurant.is_vip,restaurant.is_gold_vip,restaurant.is_important_account,restaurant.registered_date,restaurant.created_date_id,restaurant.created_date,restaurant.last_order_date,restaurant.first_order_date,restaurant.rut,restaurant.has_online_payment,restaurant.has_stamps,restaurant.has_pos,restaurant.restaurant_street,restaurant.restaurant_door_number,restaurant.restaurant_phone,restaurant.restaurant_owner_contact_name,restaurant.restaurant_owner_contact_last_name,restaurant.restaurant_owner_contact_phones,restaurant.reception_system_name,restaurant.commission,restaurant.max_shipping_amount,restaurant.min_delivery_amount as restaurant_min_delivery_amount,restaurant.publicity_cost,restaurant.automation_cost,restaurant.qty_orders,restaurant.qty_orders_confirmed,restaurant.qty_orders_rejected,restaurant.qty_orders_canceled,restaurant.qty_users,restaurant.total_amount_confirmed,restaurant.basket_size,restaurant.activity_rate,restaurant.avg_rating,restaurant.cancelation_rate,restaurant.failure_rate,restaurant.main_cousine,restaurant.qty_comments,restaurant.is_talent,restaurant.has_banner,restaurant.has_discount,restaurant.delivery_time,restaurant.max_delivery_amount,restaurant.min_shipping_amount,restaurant.qty_zones,restaurant.visits,restaurant.cvr,restaurant.acquisitions,restaurant.dt_1_15_y_30,restaurant.dt_2_30_y_45,restaurant.dt_3_45_y_60,restaurant.dt_4_60_y_90,restaurant.dt_5_90_y_120,restaurant.dt_6_24_horas,restaurant.dt_7_48_horas,restaurant.dt_8_72_horas,restaurant.dt_9_120_y_150,restaurant.dt_10_12_horas,restaurant.dt_11_150_y_180,restaurant.perc_orders_with_time_change,restaurant.qty_orders_without_time_change,restaurant.qty_orders_with_time_change,restaurant.dt_without,restaurant.is_express,restaurant.is_debtor,restaurant.contract_number,restaurant.has_mov,restaurant.qty_current_gold_vip,restaurant.has_featured_product,restaurant.qty_current_featured_product,restaurant.is_logistic,restaurant.account_owner,restaurant.address_id,restaurant.avg_speed,restaurant.is_premium,restaurant.not_use,restaurant.main_cousine_category_id,restaurant.previous_state_id,restaurant.change_state_date_id,restaurant.avg_qty_order_last_3_month,restaurant.avg_total_amount_last_3_month,restaurant.link,restaurant.logo,restaurant.reception_system_enabled,restaurant.is_day,restaurant.is_night,restaurant.city_id,restaurant.disabled_reason,restaurant.avg_food,restaurant.avg_service,restaurant.has_custom_photo_menu,restaurant.disabled_motive,restaurant.has_shipping_amount,restaurant.orders_reception_system_id,restaurant.orders_secondary_reception_system_id,restaurant.logistics_commission,restaurant.has_restaurant_portal,restaurant.dispatch_current_month,restaurant.sap_id,restaurant.commission_restaurant,restaurant.first_date_online,restaurant.delivery_type,restaurant.email,restaurant.business_type,restaurant.business_name,restaurant.billing_info_id,restaurant.is_online,restaurant.is_new_online,restaurant.is_offline,restaurant.accepts_vouchers,restaurant.is_active,restaurant.is_pending,restaurant.is_new_online_logistic,restaurant.is_chain,restaurant.is_new_registered,restaurant.url_site,restaurant.shipping_amount as restaurant_shipping_amount,restaurant.shipping_amount_is_percentage as restaurant_shipping_amount_is_percentage,restaurant.audi_load_date,delivery_zone.id as delivery_zone_id,delivery_zone.extra_time,delivery_zone.from_date,delivery_zone.is_deleted,delivery_zone.min_delivery_amount as delivery_zone_min_delivery_amount,delivery_zone.name,delivery_zone.polygon as polygon,delivery_zone.partner_id as delivery_zone_partner_id ,delivery_zone.shipping_amount as delivery_zone_shipping_amount ,delivery_zone.shipping_amount_is_percentage as delivery_zone_shipping_amount_is_percentage ,delivery_zone.surface,delivery_zone.to_date,delivery_zone.date_created,delivery_zone.last_updated,delivery_zone.delivery_time_id FROM delivery_zone JOIN restaurant on delivery_zone.partner_id = restaurant.restaurant_id where restaurant.country_id = 3")
                if app_args.pais == 'br':
                        sqlDF_address_by_country = sqlContext.sql("SELECT restaurant.restaurant_id,restaurant.restaurant_name,restaurant.restaurant_state_id,restaurant.restaurant_type_id,restaurant.area_id,restaurant.country_id,restaurant.registered_date_id,restaurant.backend_id,restaurant.is_vip,restaurant.is_gold_vip,restaurant.is_important_account,restaurant.registered_date,restaurant.created_date_id,restaurant.created_date,restaurant.last_order_date,restaurant.first_order_date,restaurant.rut,restaurant.has_online_payment,restaurant.has_stamps,restaurant.has_pos,restaurant.restaurant_street,restaurant.restaurant_door_number,restaurant.restaurant_phone,restaurant.restaurant_owner_contact_name,restaurant.restaurant_owner_contact_last_name,restaurant.restaurant_owner_contact_phones,restaurant.reception_system_name,restaurant.commission,restaurant.max_shipping_amount,restaurant.min_delivery_amount as restaurant_min_delivery_amount,restaurant.publicity_cost,restaurant.automation_cost,restaurant.qty_orders,restaurant.qty_orders_confirmed,restaurant.qty_orders_rejected,restaurant.qty_orders_canceled,restaurant.qty_users,restaurant.total_amount_confirmed,restaurant.basket_size,restaurant.activity_rate,restaurant.avg_rating,restaurant.cancelation_rate,restaurant.failure_rate,restaurant.main_cousine,restaurant.qty_comments,restaurant.is_talent,restaurant.has_banner,restaurant.has_discount,restaurant.delivery_time,restaurant.max_delivery_amount,restaurant.min_shipping_amount,restaurant.qty_zones,restaurant.visits,restaurant.cvr,restaurant.acquisitions,restaurant.dt_1_15_y_30,restaurant.dt_2_30_y_45,restaurant.dt_3_45_y_60,restaurant.dt_4_60_y_90,restaurant.dt_5_90_y_120,restaurant.dt_6_24_horas,restaurant.dt_7_48_horas,restaurant.dt_8_72_horas,restaurant.dt_9_120_y_150,restaurant.dt_10_12_horas,restaurant.dt_11_150_y_180,restaurant.perc_orders_with_time_change,restaurant.qty_orders_without_time_change,restaurant.qty_orders_with_time_change,restaurant.dt_without,restaurant.is_express,restaurant.is_debtor,restaurant.contract_number,restaurant.has_mov,restaurant.qty_current_gold_vip,restaurant.has_featured_product,restaurant.qty_current_featured_product,restaurant.is_logistic,restaurant.account_owner,restaurant.address_id,restaurant.avg_speed,restaurant.is_premium,restaurant.not_use,restaurant.main_cousine_category_id,restaurant.previous_state_id,restaurant.change_state_date_id,restaurant.avg_qty_order_last_3_month,restaurant.avg_total_amount_last_3_month,restaurant.link,restaurant.logo,restaurant.reception_system_enabled,restaurant.is_day,restaurant.is_night,restaurant.city_id,restaurant.disabled_reason,restaurant.avg_food,restaurant.avg_service,restaurant.has_custom_photo_menu,restaurant.disabled_motive,restaurant.has_shipping_amount,restaurant.orders_reception_system_id,restaurant.orders_secondary_reception_system_id,restaurant.logistics_commission,restaurant.has_restaurant_portal,restaurant.dispatch_current_month,restaurant.sap_id,restaurant.commission_restaurant,restaurant.first_date_online,restaurant.delivery_type,restaurant.email,restaurant.business_type,restaurant.business_name,restaurant.billing_info_id,restaurant.is_online,restaurant.is_new_online,restaurant.is_offline,restaurant.accepts_vouchers,restaurant.is_active,restaurant.is_pending,restaurant.is_new_online_logistic,restaurant.is_chain,restaurant.is_new_registered,restaurant.url_site,restaurant.shipping_amount as restaurant_shipping_amount,restaurant.shipping_amount_is_percentage as restaurant_shipping_amount_is_percentage,restaurant.audi_load_date,delivery_zone.id as delivery_zone_id,delivery_zone.extra_time,delivery_zone.from_date,delivery_zone.is_deleted,delivery_zone.min_delivery_amount as delivery_zone_min_delivery_amount,delivery_zone.name,delivery_zone.polygon as polygon,delivery_zone.partner_id as delivery_zone_partner_id ,delivery_zone.shipping_amount as delivery_zone_shipping_amount ,delivery_zone.shipping_amount_is_percentage as delivery_zone_shipping_amount_is_percentage ,delivery_zone.surface,delivery_zone.to_date,delivery_zone.date_created,delivery_zone.last_updated,delivery_zone.delivery_time_id FROM delivery_zone JOIN restaurant on delivery_zone.partner_id = restaurant.restaurant_id where restaurant.country_id = 5")
                if app_args.pais == 'pa':
                        sqlDF_address_by_country = sqlContext.sql("SELECT restaurant.restaurant_id,restaurant.restaurant_name,restaurant.restaurant_state_id,restaurant.restaurant_type_id,restaurant.area_id,restaurant.country_id,restaurant.registered_date_id,restaurant.backend_id,restaurant.is_vip,restaurant.is_gold_vip,restaurant.is_important_account,restaurant.registered_date,restaurant.created_date_id,restaurant.created_date,restaurant.last_order_date,restaurant.first_order_date,restaurant.rut,restaurant.has_online_payment,restaurant.has_stamps,restaurant.has_pos,restaurant.restaurant_street,restaurant.restaurant_door_number,restaurant.restaurant_phone,restaurant.restaurant_owner_contact_name,restaurant.restaurant_owner_contact_last_name,restaurant.restaurant_owner_contact_phones,restaurant.reception_system_name,restaurant.commission,restaurant.max_shipping_amount,restaurant.min_delivery_amount as restaurant_min_delivery_amount,restaurant.publicity_cost,restaurant.automation_cost,restaurant.qty_orders,restaurant.qty_orders_confirmed,restaurant.qty_orders_rejected,restaurant.qty_orders_canceled,restaurant.qty_users,restaurant.total_amount_confirmed,restaurant.basket_size,restaurant.activity_rate,restaurant.avg_rating,restaurant.cancelation_rate,restaurant.failure_rate,restaurant.main_cousine,restaurant.qty_comments,restaurant.is_talent,restaurant.has_banner,restaurant.has_discount,restaurant.delivery_time,restaurant.max_delivery_amount,restaurant.min_shipping_amount,restaurant.qty_zones,restaurant.visits,restaurant.cvr,restaurant.acquisitions,restaurant.dt_1_15_y_30,restaurant.dt_2_30_y_45,restaurant.dt_3_45_y_60,restaurant.dt_4_60_y_90,restaurant.dt_5_90_y_120,restaurant.dt_6_24_horas,restaurant.dt_7_48_horas,restaurant.dt_8_72_horas,restaurant.dt_9_120_y_150,restaurant.dt_10_12_horas,restaurant.dt_11_150_y_180,restaurant.perc_orders_with_time_change,restaurant.qty_orders_without_time_change,restaurant.qty_orders_with_time_change,restaurant.dt_without,restaurant.is_express,restaurant.is_debtor,restaurant.contract_number,restaurant.has_mov,restaurant.qty_current_gold_vip,restaurant.has_featured_product,restaurant.qty_current_featured_product,restaurant.is_logistic,restaurant.account_owner,restaurant.address_id,restaurant.avg_speed,restaurant.is_premium,restaurant.not_use,restaurant.main_cousine_category_id,restaurant.previous_state_id,restaurant.change_state_date_id,restaurant.avg_qty_order_last_3_month,restaurant.avg_total_amount_last_3_month,restaurant.link,restaurant.logo,restaurant.reception_system_enabled,restaurant.is_day,restaurant.is_night,restaurant.city_id,restaurant.disabled_reason,restaurant.avg_food,restaurant.avg_service,restaurant.has_custom_photo_menu,restaurant.disabled_motive,restaurant.has_shipping_amount,restaurant.orders_reception_system_id,restaurant.orders_secondary_reception_system_id,restaurant.logistics_commission,restaurant.has_restaurant_portal,restaurant.dispatch_current_month,restaurant.sap_id,restaurant.commission_restaurant,restaurant.first_date_online,restaurant.delivery_type,restaurant.email,restaurant.business_type,restaurant.business_name,restaurant.billing_info_id,restaurant.is_online,restaurant.is_new_online,restaurant.is_offline,restaurant.accepts_vouchers,restaurant.is_active,restaurant.is_pending,restaurant.is_new_online_logistic,restaurant.is_chain,restaurant.is_new_registered,restaurant.url_site,restaurant.shipping_amount as restaurant_shipping_amount,restaurant.shipping_amount_is_percentage as restaurant_shipping_amount_is_percentage,restaurant.audi_load_date,delivery_zone.id as delivery_zone_id,delivery_zone.extra_time,delivery_zone.from_date,delivery_zone.is_deleted,delivery_zone.min_delivery_amount as delivery_zone_min_delivery_amount,delivery_zone.name,delivery_zone.polygon as polygon,delivery_zone.partner_id as delivery_zone_partner_id ,delivery_zone.shipping_amount as delivery_zone_shipping_amount ,delivery_zone.shipping_amount_is_percentage as delivery_zone_shipping_amount_is_percentage ,delivery_zone.surface,delivery_zone.to_date,delivery_zone.date_created,delivery_zone.last_updated,delivery_zone.delivery_time_id FROM delivery_zone JOIN restaurant on delivery_zone.partner_id = restaurant.restaurant_id where restaurant.country_id = 11")
                if app_args.pais == 'py':
                        sqlDF_address_by_country = sqlContext.sql("SELECT restaurant.restaurant_id,restaurant.restaurant_name,restaurant.restaurant_state_id,restaurant.restaurant_type_id,restaurant.area_id,restaurant.country_id,restaurant.registered_date_id,restaurant.backend_id,restaurant.is_vip,restaurant.is_gold_vip,restaurant.is_important_account,restaurant.registered_date,restaurant.created_date_id,restaurant.created_date,restaurant.last_order_date,restaurant.first_order_date,restaurant.rut,restaurant.has_online_payment,restaurant.has_stamps,restaurant.has_pos,restaurant.restaurant_street,restaurant.restaurant_door_number,restaurant.restaurant_phone,restaurant.restaurant_owner_contact_name,restaurant.restaurant_owner_contact_last_name,restaurant.restaurant_owner_contact_phones,restaurant.reception_system_name,restaurant.commission,restaurant.max_shipping_amount,restaurant.min_delivery_amount as restaurant_min_delivery_amount,restaurant.publicity_cost,restaurant.automation_cost,restaurant.qty_orders,restaurant.qty_orders_confirmed,restaurant.qty_orders_rejected,restaurant.qty_orders_canceled,restaurant.qty_users,restaurant.total_amount_confirmed,restaurant.basket_size,restaurant.activity_rate,restaurant.avg_rating,restaurant.cancelation_rate,restaurant.failure_rate,restaurant.main_cousine,restaurant.qty_comments,restaurant.is_talent,restaurant.has_banner,restaurant.has_discount,restaurant.delivery_time,restaurant.max_delivery_amount,restaurant.min_shipping_amount,restaurant.qty_zones,restaurant.visits,restaurant.cvr,restaurant.acquisitions,restaurant.dt_1_15_y_30,restaurant.dt_2_30_y_45,restaurant.dt_3_45_y_60,restaurant.dt_4_60_y_90,restaurant.dt_5_90_y_120,restaurant.dt_6_24_horas,restaurant.dt_7_48_horas,restaurant.dt_8_72_horas,restaurant.dt_9_120_y_150,restaurant.dt_10_12_horas,restaurant.dt_11_150_y_180,restaurant.perc_orders_with_time_change,restaurant.qty_orders_without_time_change,restaurant.qty_orders_with_time_change,restaurant.dt_without,restaurant.is_express,restaurant.is_debtor,restaurant.contract_number,restaurant.has_mov,restaurant.qty_current_gold_vip,restaurant.has_featured_product,restaurant.qty_current_featured_product,restaurant.is_logistic,restaurant.account_owner,restaurant.address_id,restaurant.avg_speed,restaurant.is_premium,restaurant.not_use,restaurant.main_cousine_category_id,restaurant.previous_state_id,restaurant.change_state_date_id,restaurant.avg_qty_order_last_3_month,restaurant.avg_total_amount_last_3_month,restaurant.link,restaurant.logo,restaurant.reception_system_enabled,restaurant.is_day,restaurant.is_night,restaurant.city_id,restaurant.disabled_reason,restaurant.avg_food,restaurant.avg_service,restaurant.has_custom_photo_menu,restaurant.disabled_motive,restaurant.has_shipping_amount,restaurant.orders_reception_system_id,restaurant.orders_secondary_reception_system_id,restaurant.logistics_commission,restaurant.has_restaurant_portal,restaurant.dispatch_current_month,restaurant.sap_id,restaurant.commission_restaurant,restaurant.first_date_online,restaurant.delivery_type,restaurant.email,restaurant.business_type,restaurant.business_name,restaurant.billing_info_id,restaurant.is_online,restaurant.is_new_online,restaurant.is_offline,restaurant.accepts_vouchers,restaurant.is_active,restaurant.is_pending,restaurant.is_new_online_logistic,restaurant.is_chain,restaurant.is_new_registered,restaurant.url_site,restaurant.shipping_amount as restaurant_shipping_amount,restaurant.shipping_amount_is_percentage as restaurant_shipping_amount_is_percentage,restaurant.audi_load_date,delivery_zone.id as delivery_zone_id,delivery_zone.extra_time,delivery_zone.from_date,delivery_zone.is_deleted,delivery_zone.min_delivery_amount as delivery_zone_min_delivery_amount,delivery_zone.name,delivery_zone.polygon as polygon,delivery_zone.partner_id as delivery_zone_partner_id ,delivery_zone.shipping_amount as delivery_zone_shipping_amount ,delivery_zone.shipping_amount_is_percentage as delivery_zone_shipping_amount_is_percentage ,delivery_zone.surface,delivery_zone.to_date,delivery_zone.date_created,delivery_zone.last_updated,delivery_zone.delivery_time_id FROM delivery_zone JOIN restaurant on delivery_zone.partner_id = restaurant.restaurant_id where restaurant.country_id = 15")
                if app_args.pais == 'bo':
                        sqlDF_address_by_country = sqlContext.sql("SELECT restaurant.restaurant_id,restaurant.restaurant_name,restaurant.restaurant_state_id,restaurant.restaurant_type_id,restaurant.area_id,restaurant.country_id,restaurant.registered_date_id,restaurant.backend_id,restaurant.is_vip,restaurant.is_gold_vip,restaurant.is_important_account,restaurant.registered_date,restaurant.created_date_id,restaurant.created_date,restaurant.last_order_date,restaurant.first_order_date,restaurant.rut,restaurant.has_online_payment,restaurant.has_stamps,restaurant.has_pos,restaurant.restaurant_street,restaurant.restaurant_door_number,restaurant.restaurant_phone,restaurant.restaurant_owner_contact_name,restaurant.restaurant_owner_contact_last_name,restaurant.restaurant_owner_contact_phones,restaurant.reception_system_name,restaurant.commission,restaurant.max_shipping_amount,restaurant.min_delivery_amount as restaurant_min_delivery_amount,restaurant.publicity_cost,restaurant.automation_cost,restaurant.qty_orders,restaurant.qty_orders_confirmed,restaurant.qty_orders_rejected,restaurant.qty_orders_canceled,restaurant.qty_users,restaurant.total_amount_confirmed,restaurant.basket_size,restaurant.activity_rate,restaurant.avg_rating,restaurant.cancelation_rate,restaurant.failure_rate,restaurant.main_cousine,restaurant.qty_comments,restaurant.is_talent,restaurant.has_banner,restaurant.has_discount,restaurant.delivery_time,restaurant.max_delivery_amount,restaurant.min_shipping_amount,restaurant.qty_zones,restaurant.visits,restaurant.cvr,restaurant.acquisitions,restaurant.dt_1_15_y_30,restaurant.dt_2_30_y_45,restaurant.dt_3_45_y_60,restaurant.dt_4_60_y_90,restaurant.dt_5_90_y_120,restaurant.dt_6_24_horas,restaurant.dt_7_48_horas,restaurant.dt_8_72_horas,restaurant.dt_9_120_y_150,restaurant.dt_10_12_horas,restaurant.dt_11_150_y_180,restaurant.perc_orders_with_time_change,restaurant.qty_orders_without_time_change,restaurant.qty_orders_with_time_change,restaurant.dt_without,restaurant.is_express,restaurant.is_debtor,restaurant.contract_number,restaurant.has_mov,restaurant.qty_current_gold_vip,restaurant.has_featured_product,restaurant.qty_current_featured_product,restaurant.is_logistic,restaurant.account_owner,restaurant.address_id,restaurant.avg_speed,restaurant.is_premium,restaurant.not_use,restaurant.main_cousine_category_id,restaurant.previous_state_id,restaurant.change_state_date_id,restaurant.avg_qty_order_last_3_month,restaurant.avg_total_amount_last_3_month,restaurant.link,restaurant.logo,restaurant.reception_system_enabled,restaurant.is_day,restaurant.is_night,restaurant.city_id,restaurant.disabled_reason,restaurant.avg_food,restaurant.avg_service,restaurant.has_custom_photo_menu,restaurant.disabled_motive,restaurant.has_shipping_amount,restaurant.orders_reception_system_id,restaurant.orders_secondary_reception_system_id,restaurant.logistics_commission,restaurant.has_restaurant_portal,restaurant.dispatch_current_month,restaurant.sap_id,restaurant.commission_restaurant,restaurant.first_date_online,restaurant.delivery_type,restaurant.email,restaurant.business_type,restaurant.business_name,restaurant.billing_info_id,restaurant.is_online,restaurant.is_new_online,restaurant.is_offline,restaurant.accepts_vouchers,restaurant.is_active,restaurant.is_pending,restaurant.is_new_online_logistic,restaurant.is_chain,restaurant.is_new_registered,restaurant.url_site,restaurant.shipping_amount as restaurant_shipping_amount,restaurant.shipping_amount_is_percentage as restaurant_shipping_amount_is_percentage,restaurant.audi_load_date,delivery_zone.id as delivery_zone_id,delivery_zone.extra_time,delivery_zone.from_date,delivery_zone.is_deleted,delivery_zone.min_delivery_amount as delivery_zone_min_delivery_amount,delivery_zone.name,delivery_zone.polygon as polygon,delivery_zone.partner_id as delivery_zone_partner_id ,delivery_zone.shipping_amount as delivery_zone_shipping_amount ,delivery_zone.shipping_amount_is_percentage as delivery_zone_shipping_amount_is_percentage ,delivery_zone.surface,delivery_zone.to_date,delivery_zone.date_created,delivery_zone.last_updated,delivery_zone.delivery_time_id FROM delivery_zone JOIN restaurant on delivery_zone.partner_id = restaurant.restaurant_id where restaurant.country_id = 17")
                # se elimina el contenido anterior en hdfs de delivery_zone por pais
                os.system('hdfs dfs -rm -f -R /'+app_args.pais+'/delivery_zone')
                # escribe en hadoop el resultado del join en la carpeta del pais
                sqlDF_address_by_country.write.parquet('hdfs://hadoop-namenode:9000/'+app_args.pais+'/delivery_zone/', mode='overwrite', compression='snappy')
        except:
                print traceback.format_exc()
        time.sleep(1) #workaround para el bug del thread shutdown
def get_app_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("-p", "--pais", help="primeras dos letras del pais: uy, cl, ar, br, pa, py, bo, co")
        return parser.parse_args()
if __name__ == '__main__':
        app_args = get_app_args()
        funcion_dim_address(app_args)