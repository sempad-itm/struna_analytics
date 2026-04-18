import pandas as pd
import numpy as np
import firebirdsql
import clickhouse_connect
import os
from datetime import datetime

from privacy import get_anon_id, mask_org_name, normalize_address_text, mask_address

# Конфиги:

FB_CONFIG = {
    'host': 'localhost',
    'port': 3050,
    'database': './data/firebird/struna.fdb',
    'user': 'SYSDBA',
    'password': 'masterkey',
    'charset': 'WIN1251'
}

CH_CONFIG = {
    'host': 'localhost',
    'port': 8123,
    'user': 'analytics',
    'password': 'analytics',
    'database': 'struna_analytics'
}

# Подключение:

print("🔌 Подключение к базам данных...")
try:
    fb = firebirdsql.connect(**FB_CONFIG)
    ch = clickhouse_connect.get_client(**CH_CONFIG)
    print("✅ Успешно подключено.")
except Exception as e:
    print(f"❌ Ошибка подключения: {e}")
    exit()

# Загрузка справочников:

print("[1/6] Загрузка dim_objects...")

sql_defobj = """
    select d.GRP, d.MDM, d.NAIMOBJ, d.ADROBJ, d.KODORG, d.KATEG, p.PROPERTY,
           d.TIPOBJ, d.GUARD, ss.STATUS, d.BLKHARDWARE, sb.DESCR, d.CREATED, d.DATAZAKL, d.NDOG
    from DEFOBJ d
    left join PROPERTY p on d.KATEG = p.KOD
    left join SP_STATUS ss on d.GUARD = ss.KOD
    left join SP_BLKHARD sb on d.BLKHARDWARE = sb.BLKHARDWARE
"""
df_defobj = pd.read_sql(sql_defobj, fb)

# Анонимизация
df_defobj.insert(0, 'object_id_anon', df_defobj.apply(lambda r: get_anon_id(r['GRP'], r['MDM']), axis=1))

# Переименование
df_defobj.rename(columns={
    'NAIMOBJ':'object_name', 'ADROBJ':'address', 'KODORG':'client_id', 'KATEG': 'kateg_code',
    'PROPERTY':'kateg_name', 'TIPOBJ':'tipoobj', 'GUARD':'guard_code', 'STATUS':'status_name',
    'BLKHARDWARE':'hardware_code', 'DESCR': 'hardware_name', 'CREATED':'created_date',
    'DATAZAKL':'contract_date', 'NDOG':'contract_number'
}, inplace=True)

# Удаление исходных ключей
df_defobj.drop(columns=['GRP', 'MDM'], inplace=True)

# Маскировка названий
df_defobj['object_name'] = df_defobj.apply(lambda r: mask_org_name(r['object_name'], r['kateg_name']), axis=1)

# Нормализация + Маскировка адресов
df_defobj['address'] = df_defobj.apply(lambda row: mask_address(normalize_address_text(row['address']), row['kateg_name']), axis=1)

# Приведение типов
df_defobj['client_id'] = pd.to_numeric(df_defobj['client_id'], errors='coerce').astype('UInt16')
df_defobj['kateg_code'] = pd.to_numeric(df_defobj['kateg_code'], errors='coerce').astype('UInt8')
df_defobj['guard_code'] = pd.to_numeric(df_defobj['guard_code'], errors='coerce').astype('UInt8')
df_defobj['hardware_code'] = pd.to_numeric(df_defobj['hardware_code'], errors='coerce').astype('UInt8')

# Фикс дат
df_defobj['created_date'] = pd.to_datetime(df_defobj['created_date'], errors='coerce')
df_defobj['contract_date'] = pd.to_datetime(df_defobj['contract_date'], errors='coerce').dt.date
df_defobj['loaded_at'] = datetime.now()


print("[2/6] Загрузка dim_event_types...")

sql_mess = "select MESSLOW, ALARMMESS, CATEGORY, ISALARM from MESS"
df_mess = pd.read_sql(sql_mess, fb)

sql_mess_cat = "select ID, NAME from MESS_CAT"
df_mess_cat = pd.read_sql(sql_mess_cat, fb)

df_ivent = df_mess.merge(df_mess_cat, how='left', left_on='CATEGORY', right_on='ID')
df_ivent = df_ivent.rename(columns={
    'ALARMMESS': 'alarm_mess', 'NAME': 'category_name', 'CATEGORY': 'category_id',
    'MESSLOW': 'mess_low', 'ISALARM': 'is_alarm'
})[['mess_low', 'alarm_mess', 'category_id', 'category_name', 'is_alarm']]

df_ivent['mess_low'] = pd.to_numeric(df_ivent['mess_low'], errors='coerce').astype('UInt8')
df_ivent['category_id'] = pd.to_numeric(df_ivent['category_id'], errors='coerce').astype('UInt8')
df_ivent['is_alarm'] = np.where(df_ivent['is_alarm'] == '*', True, False)
df_ivent['loaded_at'] = datetime.now()


print("[3/6] Загрузка dim_outcomes...")

sql_outcome = "select KOD, OTVET from OTVET"
df_outcome = pd.read_sql(sql_outcome, fb)

df_outcome = df_outcome.rename(columns={'KOD': 'result_code', 'OTVET':'result_description'})

df_outcome['result_code'] = df_outcome['result_code'].astype('Int16')
df_outcome['loaded_at'] = datetime.now()


# Загрузка фактов:

print("[4/6] Загрузка raw_events...")

sql_events = """
    select EVENTID, DTTM, GRP, MDM, MESSLOW
    from DATA
    where EXTRACT(YEAR FROM DTTM) >= 2017
"""
df_events = pd.read_sql(sql_events, fb)

# Анонимизация
df_events['object_id_anon'] = df_events.apply(lambda r: get_anon_id(r['GRP'], r['MDM']), axis=1)

# Флаг для фильтра сирот
valid_ids = set(df_defobj['object_id_anon'])
df_events['has_object_ref'] = df_events['object_id_anon'].isin(valid_ids)

df_events.drop(columns=['GRP', 'MDM'], inplace=True)

df_events = df_events.rename(columns={
    'EVENTID': 'event_id', 'DTTM':'dttm', 'MESSLOW':'mess_low'
})

df_events['event_id'] = pd.to_numeric(df_events['event_id'], errors='coerce').astype('UInt32')
df_events['dttm'] = pd.to_datetime(df_events['dttm'], errors='coerce')
df_events['mess_low'] = pd.to_numeric(df_events['mess_low'], errors='coerce').astype('UInt8')
df_events['loaded_at'] = datetime.now()


print("📊 [5/6] Загрузка raw_alarms...")

sql_alarms = """
    select ALARMID, GRP, MDM, CREATED, PROCESSED, SENT, ARRIVED, RESULT
    from ALARM
    where EXTRACT(YEAR FROM CREATED) >= 2017
"""
df_alarms = pd.read_sql(sql_alarms, fb)

# Анонимизация
df_alarms['object_id_anon'] = df_alarms.apply(lambda r: get_anon_id(r['GRP'], r['MDM']), axis=1)

df_alarms.drop(columns=['GRP', 'MDM'], inplace=True)

df_alarms.rename(columns={
    'ALARMID': 'alarm_id', 'CREATED': 'created', 'PROCESSED': 'closed_at',
    'SENT': 'dispatched_at', 'ARRIVED': 'arrived_at', 'RESULT': 'result_code'
}, inplace=True)

df_alarms['alarm_id'] = pd.to_numeric(df_alarms['alarm_id'], errors='coerce').astype('UInt32')
df_alarms['result_code'] = pd.to_numeric(df_alarms['result_code'], errors='coerce').astype('Int16')
df_alarms['loaded_at'] = datetime.now()


print("🔗 [6/6] Загрузка fact_alarm_events...")

# Берем минимум ID из загруженных алармов, чтобы не грузить лишний мусор
min_alarm_id = int(df_alarms['alarm_id'].min())

sql_alarm_events = f"""
    select EVENTID, ALARMID
    from ALARM_EVENT
    where ALARMID >= {min_alarm_id}
"""
df_alarm_events = pd.read_sql(sql_alarm_events, fb)

df_alarm_events = df_alarm_events.rename(columns={'EVENTID':'event_id', 'ALARMID':'alarm_id'})

df_alarm_events['event_id'] = pd.to_numeric(df_alarm_events['event_id'], errors='coerce').astype('UInt32')
df_alarm_events['alarm_id'] = pd.to_numeric(df_alarm_events['alarm_id'], errors='coerce').astype('UInt32')
df_alarm_events['loaded_at'] = datetime.now()

# Вставка в кликхаус

print("💾 Вставка данных в ClickHouse...")

try:
    ch.insert_df('dim_objects', df_defobj)
    ch.insert_df('dim_event_types', df_ivent)
    ch.insert_df('dim_outcomes', df_outcome)
    ch.insert_df('raw_events', df_events)
    ch.insert_df('raw_alarms', df_alarms)
    ch.insert_df('fact_alarm_events', df_alarm_events)
    
    print("✅ ДАННЫЕ УСПЕШНО ЗАГРУЖЕНЫ!")
    
except Exception as e:
    print(f"❌ Ошибка вставки: {e}")

finally:
    fb.close()
    print("🔌 Соединения закрыты.")