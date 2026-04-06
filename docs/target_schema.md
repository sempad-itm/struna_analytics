**Цель:** Схема целевого хранилища данных в ClickHouse после миграции из Firebird.  
**Принципы:** Минимализм, денормализация для аналитики, безопасность данных.

## Общая информация

| Параметр              | Значение                                        |
| --------------------- | ----------------------------------------------- |
| **СУБД**              | ClickHouse 23.8+                                |
| **База данных**       | `struna_analytics`                              |
| **Количество таблиц** | 6 (3 DIM + 3 FACT)                              |
| **Движок**            | `ReplacingMergeTree` (идемпотентность загрузок) |
| **Партиционирование** | По месяцу для фактов (`toYYYYMM`)               |
| **Период данных**     | 2017–2019 (архив)                               |

## Архитектура DWH

 DIMENSIONS (3 таблицы, денормализованы)
• dim_objects       — объекты (DEFOBJ + 3 справочника)
• dim_event_types   — типы событий (MESS + MESS_CAT)
• dim_outcomes      — исходы инцидентов (OTVET) 

FACTS (3 таблицы)
• raw_events        — события (~650K записей)
• raw_alarms        — инциденты (~67K записей)
• fact_alarm_events — связка (~123K записей) 

**Исходные таблицы:** 9 → **Целевые таблицы:** 6 (денормализация справочников)

## DIMENSIONS (справочники)

### 1. `dim_objects` — Объекты (денормализовано)

**Источник:** `DEFOBJ` + `PROPERTY` + `SP_STATUS` + `SP_BLKHARD`

|Поле|Тип|Источник|Описание|
|---|---|---|---|
|`object_id_anon`|String|SHA256(GRP-MDM)[:8]|🔑 Анонимный ключ объекта|
|`object_name`|Nullable(String)|DEFOBJ.NAIMOBJ|Название объекта (⚠️ маскировка ПДн)|
|`address`|Nullable(String)|DEFOBJ.ADROBJ|Адрес (⚠️ маскировка ПДн)|
|`client_id`|UInt32|DEFOBJ.KODORG|Код организации|
|`kateg_code`|UInt8|DEFOBJ.KATEG|Код категории (0/1/2)|
|`kateg_name`|Nullable(String)|PROPERTY.PROPERTY|Название категории|
|`tipoobj`|Nullable(String)|DEFOBJ.TIPOBJ|Детальный тип|
|`guard_code`|UInt8|DEFOBJ.GUARD|Код статуса охраны|
|`status_name`|Nullable(String)|SP_STATUS.STATUS|Название статуса|
|`hardware_code`|Nullable(String)|DEFOBJ.BLKHARDWARE|Код оборудования|
|`hardware_name`|Nullable(String)|SP_BLKHARD.DESCR|Название оборудования|
|`created_date`|Nullable(Date)|DEFOBJ.CREATED|Дата создания карточки|
|`contract_date`|Nullable(Date)|DEFOBJ.DATAZAKL|Дата договора|
|`contract_number`|Nullable(String)|DEFOBJ.NDOG|Номер договора|
|`loaded_at`|DateTime|ETL|Время загрузки|

**Движок:** 

```sql
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (object_id_anon)
```

### 2. `dim_event_types` — Типы событий (денормализовано)

**Источник:** `MESS` + `MESS_CAT`

| Поле            | Тип              | Источник       | Описание           |
| --------------- | ---------------- | -------------- | ------------------ |
| `mess_low`      | UInt8            | MESS.MESSLOW   | 🔑 Код события     |
| `alarm_mess`    | Nullable(String) | MESS.ALARMMESS | Описание события   |
| `category_id`   | UInt8            | MESS.CATEGORY  | Код категории      |
| `category_name` | Nullable(String) | MESS_CAT.NAME  | Название категории |
| `is_alarm`      | Nullable(UInt8)  | MESS.ISALARM   | Флаг тревоги (1/0) |
| `loaded_at`     | DateTime         | ETL            | Время загрузки     |

**Движок:** 

```sql
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (mess_low)
```

### 3. `dim_outcomes` — Исходы инцидентов

**Источник:** `OTVET`

|Поле|Тип|Источник|Описание|
|---|---|---|---|
|`result_code`|Int16|OTVET.KOD|🔑 Код исхода|
|`result_description`|Nullable(String)|OTVET.OTVET|Описание исхода|
|`loaded_at`|DateTime|ETL|Время загрузки|

**Движок:** 

```sql
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (result_code)
```

## FACTS (факты)

### 4. `raw_events` — События

**Источник:** `DATA`

|Поле|Тип|Источник|Описание|
|---|---|---|---|
|`event_id`|UInt32|DATA.EVENTID|🔑 Уникальный ID события|
|`object_id_anon`|String|SHA256(GRP-MDM)|🔗 Ключ объекта (FK → dim_objects)|
|`dttm`|DateTime|DATA.DTTM|Время события на объекте|
|`mess_low`|UInt8|DATA.MESSLOW|🔗 Тип события (FK → dim_event_types)|
|`has_object_ref`|UInt8|ETL|1=объект в справочнике, 0=сирота|
|`loaded_at`|DateTime|ETL|Время загрузки|

**Движок:**

```sql
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(dttm)
ORDER BY (event_id)
```

**Особенности:**

- ~5% событий имеют `has_object_ref = 0` (нет объекта в DEFOBJ)
- Инкрементальная загрузка по `event_id`

### 5. `raw_alarms` — Инциденты

**Источник:** `ALARM`

|Поле|Тип|Источник|Описание|
|---|---|---|---|
|`alarm_id`|UInt32|ALARM.ALARMID|🔑 Уникальный ID инцидента|
|`object_id_anon`|String|SHA256(GRP-MDM)|🔗 Ключ объекта (FK → dim_objects)|
|`created`|DateTime|ALARM.CREATED|Создание инцидента (100%)|
|`closed_at`|DateTime|ALARM.PROCESSED|Закрытие инцидента (100%)|
|`dispatched_at`|Nullable(DateTime)|ALARM.SENT|Отправка наряда (~3.5%)|
|`arrived_at`|Nullable(DateTime)|ALARM.ARRIVED|Прибытие группы (~2.4%)|
|`result_code`|Nullable(Int16)|ALARM.RESULT|🔗 Исход (FK → dim_outcomes)|
|`loaded_at`|DateTime|ETL|Время загрузки|

**Движок:**

```sql
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(created)
ORDER BY (alarm_id)
```

**Особенности:**

- 96% инцидентов без выезда (`dispatched_at` / `arrived_at` = NULL)

### 6. `fact_alarm_events` — Связка событий и инцидентов

**Источник:** `ALARM_EVENT`

|Поле|Тип|Источник|Описание|
|---|---|---|---|
|`event_id`|UInt32|ALARM_EVENT.EVENTID|🔗 FK → raw_events|
|`alarm_id`|UInt32|ALARM_EVENT.ALARMID|🔗 FK → raw_alarms|
|`loaded_at`|DateTime|ETL|Время загрузки|

**Движок:**

```sql
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (alarm_id, event_id)
```

**Особенности:**

- Позволяет точную конверсию «событие → инцидент»

## Схема связей

[[Схема_связей_clickhouse]]

## ETL: Порядок загрузки

```
1 DIM (независимые)
   → dim_outcomes (OTVET)
   
2 DIM (зависимые)
   → dim_event_types (MESS + MESS_CAT)
   → dim_objects (DEFOBJ + PROPERTY + SP_STATUS + SP_BLKHARD)
   
3 FACT
   → raw_events (DATA)
   → raw_alarms (ALARM)
   
4 LINK
   → fact_alarm_events (ALARM_EVENT)
```

## Data Quality в целевой схеме

|Проблема|Решение в ClickHouse|
|---|---|
|**ПДн (NAIMOBJ, ADROBJ)**|Маскировка в ETL: город + `***`|
|**Составной ключ GRP+MDM**|Анонимизация: `SHA256(GRP-MDM)[:8]`|
|**События без объекта (~5%)**|Поле `has_object_ref` (0/1)|
|**Тестовые ключи (GRP=0, MDM=0/1)**|Маркер `object_id_anon = 'test_default'`|
|**ISALARM = '*'**|Конвертация: `'*'` → `1` в ETL|
|**RESULT без описания**|`LEFT JOIN`, NULL в `result_description`|
## Ключевые решения и обоснование

| Решение                          | Почему                                                 |
| -------------------------------- | ------------------------------------------------------ |
| **Денормализация справочников**  | Проще для Metabase, меньше JOIN                        |
| **ReplacingMergeTree**           | Идемпотентность загрузок (повторный запуск ETL)        |
| **Партиционирование по месяцу**  | Эффективное удаление старых данных, ускорение запросов |
| **ORDER BY (event_id/alarm_id)** | Быстрый поиск по ID, инкрементальная загрузка          |
| **has_object_ref**               | Прозрачность Data Quality, фильтрация сирот            |
| **object_id_anon (хеш)**         | Безопасность, исключение ПДн из ключей                 |
