-- Struna-5 DWH
-- Версия: 1.0

-- Создаем базу данных:
CREATE DATABASE IF NOT EXISTS struna_analytics;
USE struna_analytics;

-- DIMENSIONS (справочники)

--1. dim_objects (объекты)
CREATE TABLE IF NOT EXISTS dim_objects (
    object_id_anon   String,
    object_name      Nullable(String),
    address          Nullable(String),
    client_id        UInt16,
    kateg_code       UInt8,
    kateg_name       Nullable(String),
    tipoobj          Nullable(String),
    guard_code       UInt8,
    status_name      Nullable(String),
    hardware_code    Nullable(UInt8),
    hardware_name    Nullable(String),
    created_date     DateTime,
    contract_date    Nullable(Date),
    contract_number  Nullable(String),
    loaded_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (object_id_anon);

-- 2. dim_event_types (типы событий)
CREATE TABLE IF NOT EXISTS dim_event_types
(
    mess_low        UInt8,
    alarm_mess      Nullable(String),
    category_id     UInt8,
    category_name   Nullable(String),
    is_alarm        Nullable(Bool),
    loaded_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (mess_low);

-- 3. dim_outcomes (исходы инцидентов)
CREATE TABLE IF NOT EXISTS dim_outcomes
(
    result_code          Int16,
    result_description   Nullable(String),
    loaded_at            DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (result_code);

-- FACTS (факты)

-- 4. raw_events (события)
CREATE TABLE IF NOT EXISTS raw_events
(
    event_id          UInt32,
    object_id_anon    String,
    dttm              DateTime,
    mess_low          UInt8,
    has_object_ref    Bool DEFAULT TRUE,
    loaded_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(dttm)
ORDER BY (event_id);

-- 5. raw_alarms (инциденты)
CREATE TABLE IF NOT EXISTS raw_alarms
(
    alarm_id          UInt32,
    object_id_anon    String,
    created           DateTime,
    closed_at         DateTime,
    dispatched_at     Nullable(DateTime),
    arrived_at        Nullable(DateTime),
    result_code       Nullable(Int16),
    loaded_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(created)
ORDER BY (alarm_id);

-- 6. fact_alarm_events (связка событий и инцидентов)
CREATE TABLE IF NOT EXISTS fact_alarm_events
(
    event_id    UInt32,
    alarm_id    UInt32,
    loaded_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (alarm_id, event_id);