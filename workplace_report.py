# -*- coding: utf-8 -*-
"""
Created by ARTTC at 15.04.2023

Description: workplace report generation script

"""

import sys
import time
import pandas as pd
from datetime import date
import os
import shutil
from pathlib import Path
import xlwings as xw
from tqdm import tqdm

path = str(Path(os.path.expanduser('~/Desktop/Python/universal_scripts')))
sys.path.append(path)
from Database_management import get_postgres_database_configs_mac
from Database_management import PostgresSQLconnection
from Dataframe_management import EVdataframe
from Excel_management import Excel_file

import numpy as np

# # activate when changing and reloading classes from external files:
# del sys.modules[r'Virta_data_management']
# from Virta_data_management import Virta_data
# del sys.modules[r'Dataframe_management']
# from Dataframe_management import EVdataframe
# del sys.modules[r'Database_management']
# from Database_management import MicrosoftSQLconnection
# =============================================================================
# Run parameters
# =============================================================================
all_workplace_flag = True  # if set to tru, indvidual_workplace_flag should we set to False
generate_finance_report = True
individual_workplace_flag = False  # if True, then only one workplace will be generated

# =============================================================================
# User-defined variables
# =============================================================================
username='arttch'
delivery_period = {'from': '2025-03-01',
                   'until': '2025-04-01'}  # in year-month-day format, until should be first day of next month!
first_day = (pd.to_datetime(delivery_period.get("from"))).strftime('%d.%m.%Y')
last_day = ((pd.to_datetime(delivery_period.get("until"))) - pd.DateOffset(1)).strftime('%d.%m.%Y')

location_name_adhoc = 'Unibuss Alnabru'
country_name_adhoc = 'NO'

country_names = ['%NO%', '%SE%', '%DK%']

country_dic = {'%NO%': 'Norway',
               '%SE%': 'Sweden',
               '%DK%': 'Denmark'}

country_dic_adhoc = {'NO': 'Norway',
                     'SE': 'Sweden',
                     'DK': 'Denmark'}

# aws rds generate-db-auth-token --hostname prod-ev-microservices-master-postgresql.cluster-cvuzncdgyub5.eu-west-1.rds.amazonaws.com --port 5432 --username iam_readonly
user_name = 'iam_readonly'
password = r'prod-ev-microservices-master-postgresql.cluster-cvuzncdgyub5.eu-west-1.rds.amazonaws.com:5432/?Action=connect&DBUser=iam_readonly&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIAZMARU3ADCPJTPFJV%2F20250414%2Feu-west-1%2Frds-db%2Faws4_request&X-Amz-Date=20250414T071256Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEIP%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMSJHMEUCIQDAfVkX6jzEo50HrirkRRnq597kWXMit0fK6Pjhr0Ed%2FAIgCt32w%2FKcsZOKr4fsemJpKaXwgaVw2xNET5%2B5nhi2xLEqqgII%2FP%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAFGgw2NDQyODIxMTIwMDYiDAdPPlF9CL9iRxteOCr%2BATdZF55mY5pQ40ITtPX3pkoPQBtu0HrnG9TrdgmttpEyPfovemPdUwDpxLkN4LpmRQCo7GrcvDiqoNuCHcVk490tKg2vp6W2l%2FKMt2kwKn%2Bjlh028smFHe8V4y0Qnyuui6T8ZVGEFh3rYpRuuous7xTwUtgE3wQXmPDB69mM04%2BKya6Y7LjlHuQ%2BaqJBseak2398twGcpMsX5eg1djl84gy%2F7uJ5oxzshetpvYOm2qGOW%2F4IrXIIXTPUlmeScSsRAS0m%2B6a67jTYG%2F3GUw%2BClgcEugC1V686b8y1Iy6aPA5Ptj8S24Aor468eKaIsigIxbd8z%2BbXJ9OEoTTtbZ7yMIbq8b8GOp0B8U8shoCU1D66yazlAM8k0bTW42N1dAlpOUFJchBxf2MUx5GBIs9Gtg5hxr2wJ%2F4YBSrM4yFLdNgr%2Fjc%2BrP%2BTqbpVLQeoOhTGu%2Fi%2BiNx3W3iqOmTwUtvrYBdNUWtWal0qe59dWf0HtHY4vPWDhu8w4U%2BbLUPQEae1z6HUy80xuFEZaVG6RH0JwpvrCxrszGuZOyflebdVwvvIZ8ApmA%3D%3D&X-Amz-Signature=981da20f1c45971e57bf2c01e0d14d19ef4fd43ac3863868fda61401d9894bea'

keypath = str(Path(os.path.expanduser('~/shh keys/ProdTestQAenvironmentSSHPublicKey_python')))
tunnel_username = r'arttc'
tunnel_address = r'prod-bastion01a.prod.gneis.io'

configs = get_postgres_database_configs_mac()

server_name_emb = configs['emb_PROD']['dbname']
schema_name_emb = 'b2c'
database_name_emb = r'ev_process'
tunnel_host_emb = configs['emb_PROD']['host']
user_emb = configs['emb_PROD']['user']
password_emb = configs['emb_PROD']['password']

server_name_charging = configs['charging_PROD']['dbname']
schema_name_charging = configs['charging_PROD']['dbname']
database_name_charging = configs['charging_PROD']['dbname']
tunnel_host_charging = configs['charging_PROD']['host']

server_name_order = configs['order_PROD']['dbname']
schema_name_order = configs['order_PROD']['dbname']
database_name_order = configs['order_PROD']['dbname']
tunnel_host_order = configs['order_PROD']['host']

server_name_evebo = configs['ev_infrastructure_PROD']['dbname']
schema_name_evebo = 'public'
database_name_evebo = configs['ev_infrastructure_PROD']['dbname']
tunnel_host_evebo = configs['ev_infrastructure_PROD']['host']

server_name_rfid = configs['rfid_PROD']['dbname']
schema_name_rfid = configs['rfid_PROD']['dbname']
database_name_rfid = r'ev_b2b_rfid_tag'
tunnel_host_rfid = configs['rfid_PROD']['host']

server_name_trx = configs['transaction_PROD']['dbname']
schema_name_trx = 'ts'
database_name_trx = r'ts_transaction'
tunnel_host_trx = configs['transaction_PROD']['host']
user_trx = configs['transaction_PROD']['user']
password_trx = configs['transaction_PROD']['password']

list_to_exclude = ['Almedal', 'Trucks', 'Circle', 'Home', 'Ionity', 'Hubject', 'Scandic Park', 'Sørlandsparken',
                   'Dale Cafe', 'Favn Hafjell', 'Cirlce']

# old architecture querry
emb_querry = f'''
SELECT ep.id,
ep.external_transaction_id,
ep.order_id,
ep.modified,
ep.start_timestamp ,
ep.end_timestamp ,
ep.evse_id,
ep.station_name,
ep.process_cpms,
ep.process_cpo,
ep.battery_level,
ep.charging_speed,
ep.watt_hour ,
ep.duration , 
ep.creation_channel ,
business_model,
ep.user_id,
ep.rfid,
ep.status,
ep.charging_service_process_id,
pd.product_id,
pd.vat_code,
pd.kwh_price,
pd.country,
ct.corrupted_types 
FROM b2c.ev_process ep 
LEFT JOIN b2c.ev_process_pricing_details pd
ON ep.id=pd.process_id
LEFT JOIN b2c.ev_corruption_types ct
ON ep.id=ct.process_id
WHERE 1=1
and status NOT IN ('PENDING_CANCEL','CREATED','CREATED_FINGERPRINT','CREATED_REDIRECT','CREATED_CHALLENGE','STARTING','IN_PROGRESS') 
AND ep.evse_id ILIKE any (array{country_names})
AND ep.evse_id NOT ILIKE all (array['%ION%', '%IOY%', '%MER%'])
AND start_timestamp BETWEEN  '{delivery_period.get('from')} 00:00:00.000' AND '{delivery_period.get('until')} 00:00:00.000'
'''

# new architecture querry:

charging_querry = f'''
select id,
external_transaction_id,
modified,
duration,
energy,
evse_id,
token,
origin_transaction_id,
start_time,
stop_time,
status,
user_id,
cpms_name,
battery_level,
creation_channel,
charger_group_name,
cpo_name,
charger_speed,
location_type,
cpct.corrupted_reasons
from charging.charging.charging_processes cp
full join charging.charging.charger_process_corruption_types cpct
on cp.id =cpct.process_id  
where 1=1
and evse_id like any (array{country_names}) 
and cpo_name like '%CIRCLEK%' 
and start_time BETWEEN '{delivery_period.get('from')} 00:00:00.000' AND '{delivery_period.get('until')} 00:00:00.000'
'''

order_querry = f'''
select
o.id,
o.modified,
business_model,
status,
order_type,
jde_site_code,
order_id,
jde_product_display_name,
jde_product_id,
quantity,
vat_code,
reservation_amount,
total_amount,
total_amount_currency,
total_discount_amount,
total_amount_tax,
discount_name,
o.country,
ol.base_price
from order_svc.order_svc.orders o 
full join order_svc.order_svc.order_lines ol 
on o.id=ol.order_id
where service_point_id like any (array{country_names}) and start_processing_timestamp between '{delivery_period.get('from')} 00:00:00.000' AND '{delivery_period.get('until')} 00:00:00.000' and site_name ilike '%WORKPLACE%'
'''

querry_rfid = f'''SELECT rfid,card_id, partner_id FROM rfid.ev_b2b_rfid_tag
  WHERE country like any  (array{country_names}) '''

querry_deleted_rfid = f'''
select rfid,partner_id from rfid.rfid.ev_rfid_history erh 
where user_id is null and country like any (array{country_names}) 
'''

querry_ebo_company = f'''select c.partner_id ,c.name,c.partner_industry_group 
from company c 
where country like any (array{country_names}) '''

querry_ebo_evse = '''
select c.identity_key, c.id as charger_id , c.state, c.connection_date, c.usage_start_date, c.out_of_service_date,e.evse_id, c2.name as name_co , s.jde_site_code, e.max_power, c2.organization_number, ep.country, ep."name" as name_lo,
ep.business_branch
from evse e 
inner join charger c 
on e.charger_id=c.id  
inner join evse_pool ep  
on c.evse_pool_id  =ep.id 
inner join site s 
on ep.site_id =s.id
inner join company c2 
on ep.owner_company_id  =c2.id
where ep.business_branch='WORKPLACE'
'''

querry_conn_status=f'''
select cs.*, e.evse_id, c2.identity_key , ep.business_branch 
from connector_status_history cs
left join connector c 
on cs.connector_id = c.id
left join evse e 
on c.evse_id =e.id
left join charger c2 
on e.charger_id =c2.id 
left join evse_pool ep 
on c2.evse_pool_id =ep.id
where ep.business_branch ='WORKPLACE' and cs.timestamp > '{delivery_period.get('from')} 00:00:00.000' 
'''

querry_error_status=f'''
select e.evse_id, c2.identity_key, ep.business_branch, ce.creation_date, 
cged.code as general_code, cged.description as general_description,
cvsed.code as vendor_code, cvsed.description as vendor_description, cvsed.additional_information as vendor_info
from connector_error ce 
left join connector c
on ce.connector_id = c.id
left join evse e 
on c.evse_id =e.id
left join charger c2 
on e.charger_id =c2.id 
left join evse_pool ep 
on c2.evse_pool_id =ep.id 
left join connector_vendor_specific_error_dictionary cvsed 
on ce.vendor_specific_error_id = cvsed.id
left join connector_general_error_dictionary cged 
on ce.general_error_id = cged.id 
where ep.business_branch ='WORKPLACE' and ce.creation_date > '{delivery_period.get('from')} 00:00:00.000' 
'''

project_directory = str(Path(os.path.expanduser('~/Desktop/Python/Workplace_report')))
storage_directory_external = str(Path(os.path.expanduser('~/Desktop/Python/Workplace_report/Reports/For_clients')))
storage_directory_finance = str(Path(os.path.expanduser('~/Desktop/Python/Workplace_report/Reports/For_finance')))

project_server_directory = str(
    Path(os.path.expanduser(f'~/Circle K Europe/E-Mobility - 04 Segment 3 - B2B/06 Workplace charging B2B')))

evse_clean_cols = ['country', 'identity_key','state', 'charger_id', 'evse_id','connection_date','connection_year','connection_month','out_of_service_date', 'location_company_name', 'location_name', 'sim_site_code', 'max_charger_speed',
                   'location_company_orgnumber', 'business_branch']

dict_old_arch = {"modified_emb": "last_updated_timestamp",
                 "id_emb": "cdr_id",
                 'external_transaction_id': 'external_transaction_id',
                 'charging_service_process_id': 'charging_service_process_id',
                 "start_timestamp": "start_timestamp",
                 "year": "year",
                 "month": "month",
                 "day": "day",
                 "weekday": "weekday",
                 "end_timestamp": "end_timestamp",
                 "duration": "duration_seconds",
                 "kilowatt_hour": "kilowatt_hour",
                 "evse_id": "evse_id",
                 "country": "country",
                 "station_name": "location_name",
                 "location_type": "location_type",
                 "location_company_name": "location_company_name",
                 "location_company_orgnumber": "location_company_orgnumber",
                 "process_cpo": "cpo_name",
                 "jde_site_code": "sim_site_code",
                 "process_cpms": "cpms_name",
                 "charging_speed": "max_charger_speed",
                 "id_emb_duplicate": "ev_transaction_id",
                 "id_order": "invoice_reference_id",
                 "business_model_emb": "business_model",
                 "creation_channel": "creation_channel",
                 "creation_channel_duplicate": "authentication_type",
                 "emsp_name": "emsp_name",
                 "user_id": "user_id",
                 "rfid": "rfid",
                 "b2b_card_number": "b2b_card_number",
                 "b2b_card_company_name": "b2b_card_company_name",
                 "b2b_card_company_industry": "b2b_card_company_industry",
                 "b2b_card_company_partner_id": "b2b_card_company_partner_id",
                 "jde_product_id": "jde_product_id",
                 "jde_product_display_name": "product_display_name",
                 "order_type": "paid_type",
                 "reservation_amount": "reservation_amount",
                 "total_amount": "total_amount",
                 "kwh_price": "price_per_kwh",
                 "vat_code_order": "tax_rate",
                 "total_amount_tax": "total_amount_tax",
                 "total_amount_before_tax": "total_amount_before_tax",
                 "discount_name": "discount_name",
                 "total_discount_amount": "discount_amount",
                 "total_amount_currency": "currency",
                 "status_emb": "status",
                 "corrupted_types": "corrupted_reasons",
                 "battery_level": "battery_level_on_end"}

dict_new_arch = {"last_updated_timestamp": "last_updated_timestamp",
                 "id_charging": "cdr_id",
                 'external_transaction_id': 'external_transaction_id',
                 "start_time": "start_timestamp",
                 "year": "year",
                 "month": "month",
                 "day": "day",
                 "weekday": "weekday",
                 "stop_time": "end_timestamp",
                 "duration": "duration_seconds",
                 "energy": "kilowatt_hour",
                 "evse_id": "evse_id",
                 "country": "country",
                 "charger_group_name": "location_name",
                 "location_type": "location_type",
                 "location_company_name": "location_company_name",
                 "location_company_orgnumber": "location_company_orgnumber",
                 "cpo_name": "cpo_name",
                 "jde_site_code": "sim_site_code",
                 "cpms_name": "cpms_name",
                 "charger_speed": "max_charger_speed",
                 "id_charging_duplicate": "ev_transaction_id",
                 "id_order": "invoice_reference_id",
                 "business_model": "business_model",
                 "creation_channel": "creation_channel",
                 "creation_channel_duplicate": "authentication_type",
                 "emsp_name": "emsp_name",
                 "user_id": "user_id",
                 "token": "rfid",
                 "b2b_card_number": "b2b_card_number",
                 "b2b_card_company_name": "b2b_card_company_name",
                 "b2b_card_company_industry": "b2b_card_company_industry",
                 "b2b_card_company_partner_id": "b2b_card_company_partner_id",
                 "jde_product_id": "jde_product_id",
                 "jde_product_display_name": "product_display_name",
                 "order_type": "paid_type",
                 "reservation_amount": "reservation_amount",
                 "total_amount": "total_amount",
                 "base_price": "price_per_kwh",
                 "vat_code": "tax_rate",
                 "total_amount_tax": "total_amount_tax",
                 "total_amount_before_tax": "total_amount_before_tax",
                 "discount_name": "discount_name",
                 "total_discount_amount": "discount_amount",
                 "total_amount_currency": "currency",
                 "status": "status",
                 "corrupted_reasons": "corrupted_reasons",
                 "battery_level": "battery_level_on_end"}

keep_cols_cdr_report_format = ['last_updated_timestamp',
                               'cdr_id',
                               'start_timestamp',
                               'year',
                               'month',
                               'day',
                               'weekday',
                               'end_timestamp',
                               'duration_seconds',
                               'kilowatt_hour',
                               'evse_id',
                               'identity_key',
                               'country',
                               'location_name',
                               'location_type',
                               'location_company_name',
                               'location_company_orgnumber',
                               'cpo_name',
                               'sim_site_code',
                               'cpms_name',
                               'max_charger_speed',
                               'ev_transaction_id',
                               'invoice_reference_id',
                               'business_model',
                               'creation_channel',
                               'authentication_type',
                               'emsp_name',
                               'user_id',
                               'rfid',
                               'b2b_card_number',
                               'b2b_card_company_name',
                               'b2b_card_company_industry',
                               'b2b_card_company_partner_id',
                               'jde_product_id',
                               'product_display_name',
                               'paid_type',
                               'reservation_amount',
                               'total_amount',
                               'price_per_kwh',
                               'tax_rate',
                               'total_amount_tax',
                               'total_amount_before_tax',
                               'discount_name',
                               'discount_amount',
                               'currency',
                               'status',
                               'corrupted_reasons',
                               'battery_level_on_end']


# =============================================================================
# Run parameters
# =============================================================================
# add base price
# add folder structure
# add statistics report

# =============================================================================
# User-defined functions
# =============================================================================
def clean_company_data(df_company, dict_new_arch):
    df_compamy_clean = df_company.loc[df_company['partner_id'].isna() == False]
    df_compamy_clean.drop_duplicates(subset=['partner_id'], keep='first', inplace=True)
    df_compamy_clean.rename(columns={'name': dict_new_arch.get('b2b_card_company_name'),
                                     'partner_industry_group': dict_new_arch.get('b2b_card_company_industry')},
                            inplace=True)
    print('SUCESS clean_company_data')
    return df_compamy_clean


def clean_evse_data(df_evse, evse_clean_cols):
    df_evse_clean = df_evse.loc[df_evse['evse_id'].isna() == False]
    df_evse_clean.drop_duplicates(subset=['evse_id'], keep='first', inplace=True)
    df_evse_clean['connection_year']=df_evse_clean['connection_date'].dt.year
    df_evse_clean['connection_month'] = df_evse_clean['connection_date'].dt.month
    df_evse_clean.rename(
        columns={'name_co': 'location_company_name', 'organization_number': 'location_company_orgnumber',
                 'jde_site_code': 'sim_site_code', 'max_power': 'max_charger_speed', 'name_lo': 'location_name'},
        inplace=True)
    df_evse_clean = df_evse_clean[evse_clean_cols]
    print('SUCESS clean_evse_data')
    return df_evse_clean

def add_conn_status_and_error(df_evse_clean,df_conn_status,df_error_status):
    df=df_evse_clean.copy()
    df_conn_status['connector_status_timestamp']=df_conn_status['timestamp']
    df_error_status['error_status_timestamp']=df_error_status['creation_date']
    df_conn_status_latest=df_conn_status.loc[df_conn_status.groupby('evse_id')['connector_status_timestamp'].idxmax()]
    df_error_status_latest=df_error_status.loc[df_error_status.groupby('evse_id')['error_status_timestamp'].idxmax()]
    df_conn_status_latest=df_conn_status_latest[['evse_id','connector_status_timestamp','connector_status']]
    df_error_status_latest=df_error_status_latest[['evse_id','error_status_timestamp','general_code','general_description','vendor_code','vendor_description']]
    df=df.merge(df_conn_status_latest,how='left',on='evse_id')
    df=df.merge(df_error_status_latest,how='left',on='evse_id')
    df.loc[df['connector_status'].isna(),'connector_status']='UNKNOWN'
    return df

def add_conn_status_and_error_chargers(df_chargers_clean,df_conn_status,df_error_status):
    df = df_chargers_clean.copy()
    df_conn_status['connector_status_timestamp'] = df_conn_status['timestamp']
    df_error_status['error_status_timestamp'] = df_error_status['creation_date']
    df_conn_status_latest = df_conn_status.loc[df_conn_status.groupby('identity_key')['connector_status_timestamp'].idxmax()]
    df_error_status_latest = df_error_status.loc[df_error_status.groupby('identity_key')['error_status_timestamp'].idxmax()]
    df_conn_status_latest = df_conn_status_latest[['identity_key', 'connector_status_timestamp', 'connector_status']]
    df_error_status_latest = df_error_status_latest[
        ['identity_key', 'error_status_timestamp', 'general_code', 'general_description', 'vendor_code',
         'vendor_description']]
    df = df.merge(df_conn_status_latest, how='left', on='identity_key')
    df = df.merge(df_error_status_latest, how='left', on='identity_key')
    df.loc[df['connector_status'].isna(), 'connector_status'] = 'UNKNOWN'
    return df

def create_data_frame_old_architecture(df_emb, df_order, dict_old_arch):
    df_old_architecture = pd.merge(df_emb, df_order, how='left', on='order_id', suffixes=('_emb', '_order'))
    df_old_architecture['kilowatt_hour'] = (df_old_architecture['watt_hour'] / 1000).round(2)
    df_old_architecture['kilowatt_hour'] = np.where(~df_old_architecture['quantity'].isna(),
                                                    df_old_architecture['quantity'],
                                                    df_old_architecture['kilowatt_hour'])
    df_old_architecture['id_emb_duplicate'] = df_old_architecture['id_emb'].copy()
    df_old_architecture['creation_channel_duplicate'] = df_old_architecture['creation_channel'].copy()
    df_old_architecture['location_type'] = np.nan
    df_old_architecture['year'] = np.nan
    df_old_architecture['month'] = np.nan
    df_old_architecture['day'] = np.nan
    df_old_architecture['weekday'] = np.nan
    df_old_architecture['country'] = np.nan
    df_old_architecture['location_company_name'] = np.nan
    df_old_architecture['location_company_orgnumber'] = np.nan
    df_old_architecture['emsp_name'] = np.nan
    df_old_architecture['b2b_card_number'] = np.nan
    df_old_architecture['b2b_card_company_name'] = np.nan
    df_old_architecture['b2b_card_company_industry'] = np.nan
    df_old_architecture['b2b_card_company_partner_id'] = np.nan
    df_old_architecture['total_amount_before_tax'] = np.nan
    df_old_architecture.rename(columns=dict_old_arch, inplace=True)
    df_old_architecture = df_old_architecture[list(dict_old_arch.values())]
    print('SUCESS create_data_frame_old_architecture')
    return df_old_architecture


def create_data_frame_new_architecture(df_charging, df_order, dict_new_arch):
    df_new_architecture = pd.merge(df_charging, df_order, how='left', left_on='origin_transaction_id', right_on='id',
                                   suffixes=('_charging', '_order'))
    df_new_architecture['last_updated_timestamp'] = np.where(df_new_architecture['modified_charging'].isna(),
                                                             df_new_architecture['modified_order'],
                                                             df_new_architecture['modified_charging'])
    df_new_architecture['status'] = np.where(df_new_architecture['status_order'].isna(),
                                             df_new_architecture['status_charging'],
                                             df_new_architecture['status_order'])
    df_new_architecture.loc[df_new_architecture['corrupted_reasons'].isna() == False, 'status'] = 'CORRUPTED'
    df_new_architecture['id_charging_duplicate'] = df_new_architecture['id_charging'].copy()
    df_new_architecture['creation_channel_duplicate'] = df_new_architecture['creation_channel'].copy()
    df_new_architecture['year'] = np.nan
    df_new_architecture['month'] = np.nan
    df_new_architecture['day'] = np.nan
    df_new_architecture['weekday'] = np.nan
    df_new_architecture['country'] = np.nan
    df_new_architecture['location_company_name'] = np.nan
    df_new_architecture['location_company_orgnumber'] = np.nan
    df_new_architecture['emsp_name'] = np.nan
    df_new_architecture['b2b_card_number'] = np.nan
    df_new_architecture['b2b_card_company_name'] = np.nan
    df_new_architecture['b2b_card_company_industry'] = np.nan
    df_new_architecture['b2b_card_company_partner_id'] = np.nan
    df_new_architecture['total_amount_before_tax'] = np.nan
    df_new_architecture.rename(columns=dict_new_arch, inplace=True)
    df_new_architecture.drop_duplicates(subset='external_transaction_id',
                                        inplace=True)  # removing duplicate corrupted transactions with two corrupted statuses per transaction
    df_new_architecture = df_new_architecture[list(dict_new_arch.values())]
    print('SUCESS create_data_frame_new_architecture')
    return df_new_architecture


def create_cdr_report(df_new_architecture, df_old_architecture, df_evse_clean, df_company_clean, df_rfid,
                      keep_cols_cdr_report_format):
    # MERGE OLD AND NEW ARCHITECTURE

    df_old_architecture_pure = df_old_architecture.loc[df_old_architecture['charging_service_process_id'].isna()]
    df_cdr = pd.concat([df_new_architecture, df_old_architecture_pure], axis=0).reset_index(
        drop=True)  # concatenating old and new architecture
    # flag if external_transaction_id is duplicated
    df_cdr['duplicated'] = df_cdr['external_transaction_id'].duplicated(keep=False)
    df_cdr_driivz = df_cdr.loc[df_cdr['external_transaction_id'].isna() == False]
    df_cdr_driivz.reset_index(drop=True, inplace=True)
    df_cdr_driivz.drop_duplicates(subset='external_transaction_id', keep='first',
                                  inplace=True)  # dropping duplicates of driivz external trx id and keeping trx from the new architecture
    df_cdr_no_driivz = df_cdr.loc[df_cdr['external_transaction_id'].isna()]
    df_cdr_no_driivz.reset_index(drop=True, inplace=True)
    df_cdr = pd.concat([df_cdr_driivz, df_cdr_no_driivz], axis=0).reset_index(
        drop=True)  # concatenate driivz and no driivz
    df_cdr['year'] = df_cdr['start_timestamp'].dt.year
    df_cdr['month'] = df_cdr['start_timestamp'].dt.month
    df_cdr['day'] = df_cdr['start_timestamp'].dt.day
    df_cdr['weekday'] = df_cdr['start_timestamp'].dt.dayofweek + 1  # 1 as Monday, 7 as Sunday
    df_cdr['total_amount_before_tax'] = df_cdr['total_amount'] - df_cdr[
        'total_amount_tax']  # calculating total amount before tax
    # dropping this as we fetch from df_evse_clean, df_rfid and df_company_clean:
    df_cdr.drop(columns=['country', 'location_company_name', 'location_company_orgnumber',
                         'b2b_card_number', 'b2b_card_company_partner_id',
                         'b2b_card_company_name', 'b2b_card_company_industry', 'max_charger_speed', 'sim_site_code'],
                inplace=True)
    df_cdr = df_cdr.loc[df_cdr['status'] != 'IN_PROGRESS']  # removing IN_PROGRESS transactions
    df_cdr = df_cdr.loc[df_cdr['status'] != 'PROCESSING']  # removing PROCESSING transactions
    df_cdr.reset_index(drop=True, inplace=True)
    df_cdr = pd.merge(df_cdr, df_evse_clean, how='left', on='evse_id')  # adding evse owner data
    df_cdr = pd.merge(df_cdr, df_rfid, on='rfid', how='left')  # adding company card data
    df_cdr = pd.merge(df_cdr, df_company_clean, on='partner_id', how='left')  # adding company card owner data
    df_cdr.rename(columns={'card_id': 'b2b_card_number', 'partner_id': 'b2b_card_company_partner_id'}, inplace=True)
    df_cdr['location_name'] = np.where(df_cdr['location_name_x'].isna(), df_cdr['location_name_y'],
                                       df_cdr['location_name_x'])
    df_cdr['location_name'] = np.where(df_cdr['location_name'] == 'Unknown', df_cdr['location_name_y'],
                                       df_cdr['location_name'])
    df_cdr = df_cdr[keep_cols_cdr_report_format]  # keeping only relevant columns
    df_cdr.loc[df_cdr['location_name'].isna(), 'location_name'] = 'Unknown'
    df_cdr = df_cdr.loc[~df_cdr['location_name'].str.contains('|'.join(list_to_exclude), case=False)]  # exlude from df
    print('SUCESS create_cdr_report')
    return df_cdr


def get_customer_friendly_report(df_cdr):
    df_report = df_cdr.copy()

    # MODIFYING report df to get customer friendly results:
    # where rfid is not null and business model is null, business model is 'B2C'
    df_report.loc[
        (df_report['rfid'].isna() == False) & (df_report['b2b_card_company_name'].isna()), 'business_model'] = 'B2C'
    # where rfid is OPEN, Business model is unknown
    df_report.loc[df_report['rfid'] == 'OPEN', 'business_model'] = 'UNKNOWN'
    # where rfid and b2b_card_owner are not null, business model is 'B2B'
    df_report.loc[
        (df_report['rfid'].isna() == False) & (
                    df_report['b2b_card_company_name'].isna() == False), 'business_model'] = 'B2B'
    # where business_model is B2C and user_id is empty, business model is "UNKNOWN"
    df_report.loc[(df_report['business_model'] == 'B2C') & (df_report['user_id'].isna()), 'business_model'] = 'UNKNOWN'
    # add  ' in front of b2b_card_number
    df_report['b2b_card_number'] = "'" + df_report['b2b_card_number']
    df_report.sort_values(by=['start_timestamp'], inplace=True)  # sorting by start timestamp
    df_report.reset_index(drop=True, inplace=True)  # resetting index

    # timestamps remove timezone
    df_report['last_updated_timestamp'] = df_report['last_updated_timestamp'].dt.tz_localize(None)
    df_report['start_timestamp'] = df_report['start_timestamp'].dt.tz_localize(None)
    df_report['end_timestamp'] = df_report['end_timestamp'].dt.tz_localize(None)

    # replace too big tranasctions with 0

    df_report.loc[df_report['kilowatt_hour'] > 1000, 'kilowatt_hour'] = 0

    print('SUCESS create_final_customer_friendly_report')
    return df_report


def save_company_reports(df_trx_wp_location, storage_directory_external,
                         file_name_given_company):
    with pd.ExcelWriter(
            storage_directory_external + '/' + f'{df_wp_location["location_name"][0][0:50]}' + '/' + file_name_given_company) as writer:
        df_trx_wp_location.to_excel(writer, 'Transactions', index=False)
        for column in df_trx_wp_location:
            column_length = max(df_trx_wp_location[column].astype(str).map(len).max(), len(column))
            col_idx = df_trx_wp_location.columns.get_loc(column)
            writer.sheets['Transactions'].set_column(col_idx, col_idx, column_length)


# =============================================================================
# Code
# =============================================================================
Tmain = time.time()

# fetch from old architecture

emb_connection = PostgresSQLconnection(server_name_emb, schema_name_emb, database_name_emb, keypath, tunnel_username,
                                       tunnel_address, tunnel_host_emb, user_emb, password_emb)
emb_connection.get_ssh_key()
emb_connection.open_ssh_tunnel()
emb_connection.get_connection_parameters()
emb_connection.connect_to_postgres_sql()
df_emb = emb_connection.sql_to_df(emb_querry)
emb_connection.postgresql_disconnect()
emb_connection.close_ssh_tunnel()

# fetch from new architecture
# charging
charging_connection = PostgresSQLconnection(server_name_charging, schema_name_charging, database_name_charging, keypath,
                                            tunnel_username, tunnel_address, tunnel_host_charging, user_name, password)
charging_connection.get_ssh_key()
charging_connection.open_ssh_tunnel()
charging_connection.get_connection_parameters()
charging_connection.connect_to_postgres_sql()
df_charging = charging_connection.sql_to_df(charging_querry)
charging_connection.postgresql_disconnect()
charging_connection.close_ssh_tunnel()

# order
order_connection = PostgresSQLconnection(server_name_order, schema_name_order, database_name_order, keypath,
                                         tunnel_username, tunnel_address, tunnel_host_order, user_name, password)
order_connection.get_ssh_key()
order_connection.open_ssh_tunnel()
order_connection.get_connection_parameters()
order_connection.connect_to_postgres_sql()
df_order = order_connection.sql_to_df(order_querry)
order_connection.postgresql_disconnect()
order_connection.close_ssh_tunnel()

# fetch company data and evse data from ebo

evebo_connection = PostgresSQLconnection(server_name_evebo, schema_name_evebo, database_name_evebo, keypath,
                                         tunnel_username, tunnel_address, tunnel_host_evebo, user_name, password)
evebo_connection.get_ssh_key()
evebo_connection.open_ssh_tunnel()
evebo_connection.get_connection_parameters()
evebo_connection.connect_to_postgres_sql()
df_company = evebo_connection.sql_to_df(querry_ebo_company)
df_evse = evebo_connection.sql_to_df(querry_ebo_evse)
df_conn_status=evebo_connection.sql_to_df(querry_conn_status)
df_error_status=evebo_connection.sql_to_df(querry_error_status)
evebo_connection.postgresql_disconnect()
evebo_connection.close_ssh_tunnel()
df_company_clean = clean_company_data(df_company, dict_new_arch)
df_evse_clean = clean_evse_data(df_evse, evse_clean_cols)
df_chargers_clean=df_evse_clean.drop_duplicates(subset=['charger_id'], keep='first')

df_evse_clean=add_conn_status_and_error(df_evse_clean,df_conn_status,df_error_status)
df_chargers_clean=add_conn_status_and_error_chargers(df_chargers_clean,df_conn_status,df_error_status)


# fetch rfid
rfid_connection = PostgresSQLconnection(server_name_rfid, schema_name_rfid, database_name_rfid, keypath,
                                        tunnel_username, tunnel_address, tunnel_host_rfid, user_name, password)
rfid_connection.get_ssh_key()
rfid_connection.open_ssh_tunnel()
rfid_connection.get_connection_parameters()
rfid_connection.connect_to_postgres_sql()
df_rfid = rfid_connection.sql_to_df(querry_rfid)
df_deleted_rfid = rfid_connection.sql_to_df(querry_deleted_rfid)  # data about deleted B2B RFID
df_rfid = pd.concat([df_rfid, df_deleted_rfid], ignore_index=True, sort=False)
df_rfid.drop_duplicates(subset=['rfid'], keep='first', inplace=True)
rfid_connection.postgresql_disconnect()
rfid_connection.close_ssh_tunnel()

# OLD ARCHITECTURE
df_old_architecture = create_data_frame_old_architecture(df_emb, df_order, dict_old_arch)

# NEW ARCHITECTURE
df_new_architecture = create_data_frame_new_architecture(df_charging, df_order, dict_new_arch)

# MERGE OLD AND NEW ARCHITECTURE
df_cdr = create_cdr_report(df_new_architecture, df_old_architecture, df_evse_clean, df_company_clean, df_rfid,
                           keep_cols_cdr_report_format)
df_cdr=df_cdr.loc[df_cdr['location_type']!='HOME']

df_report = get_customer_friendly_report(df_cdr)

if all_workplace_flag:
    # statistics excel

    # copy driver_reimbursement_overview.xlsx from server to local directory
    workplace_statistics_file = Excel_file(project_server_directory, r'Workplace_statistics_spreadsheet.xlsx',
                                           project_server_directory + '/' + r'Workplace_statistics_spreadsheet.xlsx',
                                           'None', 'None')
    workplace_statistics_file.copy_file(project_directory, full_file_path_flag=True)
    transactions_df = EVdataframe(df_report, 'tranasctions')
    wpevseids_df = EVdataframe(df_evse_clean, 'evseids')
    wpchargers_df = EVdataframe(df_chargers_clean, 'chargers')

    transactions_df.save_to_excel_below_existing_data(df_report, project_directory,
                                                      r'Workplace_statistics_spreadsheet.xlsx', 'Transactions')
    wpevseids_df.save_to_excel(df_evse_clean, project_directory, r'Workplace_statistics_spreadsheet.xlsx', 'Evseids',
                                'A1')
    wpchargers_df.save_to_excel(df_chargers_clean, project_directory, r'Workplace_statistics_spreadsheet.xlsx', 'Chargers',
                               'A1')
    workplace_statistics_file = Excel_file(project_directory, r'Workplace_statistics_spreadsheet.xlsx',
                                           project_directory + '/' + r'Workplace_statistics_spreadsheet.xlsx', 'None',
                                           'None')
    workplace_statistics_file.copy_file(project_server_directory, full_file_path_flag=True)
    workplace_statistics_file.delete_file(full_file_path_flag=False)

    for country_name in tqdm(country_names):
        # country_name=country_names[0]
        country_name_pure = ''.join(filter(str.isalpha, country_name))
        server_storage_directory_external = np.nan
        server_storage_directory_finance = np.nan
        server_storage_directory_external = str(Path(os.path.expanduser(
            f'~/Circle K Europe/E-Mobility - 04 Segment 3 - B2B/06 Workplace charging B2B/B2B Workplace {country_dic.get(country_name)}/Reports/For_clients')))
        server_storage_directory_finance = str(Path(os.path.expanduser(
            f'~/Circle K Europe/E-Mobility - 04 Segment 3 - B2B/06 Workplace charging B2B/B2B Workplace {country_dic.get(country_name)}/Reports/For_finance')))

        # finance excel
        if generate_finance_report:
            # finance excel
            df_report_cntr = df_report.loc[df_report['country'] == country_name_pure]
            file_name_finance = f'Finance_all_workplaces_report_{country_name_pure}_from_{first_day}-to_{last_day}_on_{date.today()}'
            report_file_finance = EVdataframe(df_report_cntr, file_name_finance)
            report_file_finance.save_to_new_excel(df_report_cntr, storage_directory_finance,
                                                  f'{file_name_finance}.xlsx',
                                                  'Transactions', 'A1')
            report_file_finance = Excel_file(storage_directory_finance, f'{report_file_finance.name}.xlsx',
                                             storage_directory_finance + '/' + f'{report_file_finance.name}.xlsx',
                                             'None',
                                             'None')
            report_file_finance.copy_file(server_storage_directory_finance, full_file_path_flag=True)
            report_file_finance.delete_file(full_file_path_flag=False)
            df_report_cntr = df_report_cntr.loc[df_report_cntr['kilowatt_hour'] > 0]
            df_report_cntr = df_report_cntr.loc[
                df_report_cntr['corrupted_reasons'] != 'CORRUPTED_BY_SHORT_TRANSACTIONS']

        for location in tqdm(df_report_cntr['location_name'].dropna().drop_duplicates().to_list()):
            # loc_list=df_report_cntr['location_name'].dropna().drop_duplicates().to_list()
            # location=loc_list[0]
            df_wp_location = df_report_cntr[df_report_cntr['location_name'] == location]
            df_wp_location['location_name'] = df_wp_location['location_name'].str.strip()
            df_wp_location['location_name'].drop_duplicates(keep='first', inplace=True)
            # replace special caracters in location name
            df_wp_location['location_name'].replace(to_replace='[^A-Za-zæåøäöÆÅØÄÖ0-9 ]+', value='', inplace=True,
                                                    regex=True)
            df_wp_location.reset_index(inplace=True)
            df_trx_wp_location = df_report_cntr[df_report_cntr['location_name'] == location]
            df_trx_wp_location.reset_index(inplace=True)
            # check if folder exists
            if not os.path.exists(f'{storage_directory_external}/{df_wp_location["location_name"][0][0:50]}'):
                os.makedirs(f'{storage_directory_external}/{df_wp_location["location_name"][0][0:50]}')
            file_name_given_company = f'WP_report_%s_%s_%s.xlsx' % (first_day.split('.')[2],
                                                                    first_day.split('.')[1],
                                                                    df_wp_location['location_name'][0][0:50]
                                                                    )
            df_trx_wp_location['last_updated_timestamp'] = df_trx_wp_location['last_updated_timestamp'].dt.tz_localize(
                None)
            df_trx_wp_location['start_timestamp'] = df_trx_wp_location['start_timestamp'].dt.tz_localize(None)
            df_trx_wp_location['end_timestamp'] = df_trx_wp_location['end_timestamp'].dt.tz_localize(None)

            save_company_reports(df_trx_wp_location, storage_directory_external,
                                 file_name_given_company)

            # copy all files and folders in companies_local_storage_directory to companies_server_storage_directory
            shutil.copytree(storage_directory_external, server_storage_directory_external, dirs_exist_ok=True)
            # delete all files and folders in companies_local_storage_directory
            shutil.rmtree(storage_directory_external)
            os.makedirs(storage_directory_external)

if individual_workplace_flag:
    server_storage_directory_external = str(Path(os.path.expanduser(
        f'~/Circle K Europe/E-Mobility - 04 Segment 3 - B2B/06 Workplace charging B2B/B2B Workplace {country_dic_adhoc.get(country_name_adhoc)}/Reports/For_clients')))
    file_name_adhoc = f'{location_name_adhoc}_{country_name_adhoc}_adhoc_report_from_{first_day}-to_{last_day}_on_{date.today()}'
    df_report_adhoc = df_report.loc[df_report['location_name'] == location_name_adhoc]
    report_file_adhoc = EVdataframe(df_report_adhoc, file_name_adhoc)
    report_file_adhoc.save_to_new_excel(df_report_adhoc, storage_directory_external, f'{file_name_adhoc}.xlsx',
                                        'Transactions', 'A1')
    report_file_adhoc = Excel_file(storage_directory_external, f'{report_file_adhoc.name}.xlsx',
                                   storage_directory_external + '/' + f'{report_file_adhoc.name}.xlsx', 'None', 'None')
    report_file_adhoc.copy_file(server_storage_directory_external, full_file_path_flag=True)
    report_file_adhoc.delete_file(full_file_path_flag=False)

print(f'Processed in {(time.time() - Tmain) / 60} minutes')
