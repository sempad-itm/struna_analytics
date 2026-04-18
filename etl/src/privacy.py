import re
import hashlib
from typing import Any, Optional

import pandas as pd


BUSINESS_MARKERS = {
    'ип', 'ао', 'пао', 'аптек', 'магазин', 'маг', 'торг', 'строй',
    'сервис', 'авто', 'кафе', 'ресто', 'гостин', 'склад', 'управл',
    'помещен', 'школ', 'больн', 'мбоу', 'гос', 'ооо'
}

# Паттерн ФИО: Фамилия + Пробел + Инициал. + Пробел + Инициал.
FIO_PATTERN = re.compile(r'[А-ЯЁ][а-яё\-]+\s+[А-ЯЁ]\.\s*[А-ЯЁ]\.')


def get_anon_id(grp: Any, mdm: Any) -> str:
    """
    Генерирует анонимный идентификатор объекта на основе составного ключа.
    
    Args:
        grp: Номер группы (может быть int, str, None)
        mdm: Номер ячейки (может быть int, str, None)
        
    Returns:
        Строка: SHA256-хеш [:8] или служебные маркеры ('system_id', 'unknown_id')
    """
    if grp is None or mdm is None:
        return "unknown_id"
    
    try:
        grp_val = int(grp)
    except (ValueError, TypeError):
        grp_val = -1  # фоллбэк для невалидных ключей

    if grp_val == 0:
        return "system_id"

    raw_str = f"{grp_val}-{mdm}"
    return hashlib.sha256(raw_str.encode()).hexdigest()[:8]


def mask_org_name(name: Any, kateg_name: Any) -> Optional[str]:
    """
    Контекстная маскировка названий объектов.
    Сохраняет юрлица, маскирует физлица и неоформленные ИП.
    """
    if pd.isna(name) or not str(name).strip():
        return None
    
    name_str = str(name).strip()
    kateg_str = str(kateg_name).strip().lower() if pd.notna(kateg_name) else ''
    name_lower = name_str.lower()
    
    # 1. Жилые помещения (Квартиры/МХИГ) -> Строго маскируем
    if kateg_str in ('квартира', 'мхиг'):
        return 'Частное лицо'
    
    # 2. Явное указание ИП
    if 'ип' in name_lower:
        return 'ИП'

    # 3. Поиск ФИО в строке
    has_fio = bool(FIO_PATTERN.search(name_str))
    
    # Проверка на бизнес-контекст
    is_business = any(marker in name_lower for marker in BUSINESS_MARKERS)

    if has_fio:
        # Есть ФИО + есть бизнес-слова -> Считаем это ИП без приставки
        if is_business:
            return 'ИП'
        # Есть ФИО + нет бизнес-слов -> Физическое лицо
        return 'Физическое лицо'
     
    # 4. Чистый бизнес (юрлица, госучреждения) -> Оставляем для аналитики
    return name_str


def normalize_address_text(address: Any) -> Optional[str]:
    """
    Лёгкая нормализация адреса: убирает шум, стандартизирует разделители.
    """
    if pd.isna(address):
        return None
    
    addr = str(address).strip()
    
    # 1. Латиница → Кириллица
    addr = addr.replace('c.', 'с.').replace('C.', 'С.')
    
    # 2. Нормализуем пробелы вокруг знаков препинания
    addr = re.sub(r'\s*([.,:;])\s*', r'\1 ', addr)
    
    # 3. Убираем двойные+ пробелы
    addr = re.sub(r'\s+', ' ', addr)
    
    # 4. Стандартизируем префиксы населённых пунктов
    addr = re.sub(r'^(с|пос|г|д|ст|х)\.?\s*', r'\1. ', addr, flags=re.IGNORECASE)
    
    return addr.strip()


def mask_address(address: Any, kateg_name: Any = None) -> Optional[str]:
    """
    Маскировка адреса: оставляет НП + улицу, скрывает номер дома/квартиры.
    Для жилых объектов (квартиры/МХИГ) оставляет только НП.
    """
    if pd.isna(address) or not str(address).strip():
        return None

    addr = re.sub(r'\s+', ' ', str(address).strip())
    kateg_str = str(kateg_name).strip().lower() if pd.notna(kateg_name) else ''

    # Разделяем по маркерам улицы/переулка или по запятой
    parts = re.split(r'\s+(?=ул\.|пер\.|пр\.|бул\.|ш\.|пл\.|наб\.|кв\.)|,', addr, maxsplit=1)
    settlement = parts[0].strip().rstrip(',')

    # Для квартир/МХИГ — только населённый пункт
    if kateg_str in ('квартира', 'мхиг'):
        return f"{settlement}, ***"

    # Для остальных — оставляем улицу, убираем номер дома
    if len(parts) > 1:
        street_part = parts[1].strip().split(',')[0]
        
        # Убираем всё после последней буквы перед цифрами/скобками/кавычками/дробью
        street_clean = re.sub(
            r'([А-Яа-яЁё])\s*[\d\(\)\"\'\«\»/\\].*$',
            r'\1', 
            street_part
        ).strip()
        
        # Убираем trailing запятые, точки, лишние пробелы
        street_clean = re.sub(r'[,.\s]+$', '', street_clean).strip()
        
        if street_clean and len(street_clean) > 3:
            return f"{settlement}, {street_clean}"
    
    # Фоллбэк: только населённый пункт
    return f"{settlement}, ***"