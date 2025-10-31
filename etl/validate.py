# etl/validate.py
import pandas as pd
import psycopg2
from typing import Dict


def validate_raw_data(df: pd.DataFrame) -> bool:
    print("\n" + "=" * 30)
    print("ВАЛИДАЦИЯ СЫРЫХ ДАННЫХ")
    print("=" * 30)
    
    # Проверяем что DataFrame не пустой
    if df.empty:
        raise ValueError("DataFrame пустой")
    print("✓ DataFrame не пустой")
    
    # Проверяем наличие обязательных колонок
    required_columns = ['Concentration new', 'Duration after transfection new']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Отсутствуют обязательные колонки: {missing_columns}")
    print(" Все обязательные колонки присутствуют")
    
    # Проверяем что есть данные
    if len(df) == 0:
        raise ValueError("Нет данных для обработки")
    print(f" Количество строк: {len(df)}")
    
    print(" Валидация сырых данных пройдена")
    return True


def validate_transformed_data(df: pd.DataFrame) -> bool:

    print("\n" + "=" * 30)
    print("ВАЛИДАЦИЯ ПРЕОБРАЗОВАННЫХ ДАННЫХ")
    print("=" * 30)
    
    # Проверяем типы данных числовых колонок
    numeric_columns = ['Concentration new', 'Duration after transfection new']
    
    for col in numeric_columns:
        if col in df.columns:
            if not pd.api.types.is_numeric_dtype(df[col]):
                print(f" Колонка {col} не числового типа: {df[col].dtype}")
            else:
                print(f" Колонка {col} числового типа: {df[col].dtype}")
    
    # Проверяем отсутствие NaN в ключевых колонках
    key_columns = ['Target gene', 'Cell or Organism used']
    for col in key_columns:
        if col in df.columns:
            nan_count = df[col].isna().sum()
            if nan_count > 0:
                print(f" В колонке {col} найдено {nan_count} NaN значений")
            else:
                print(f" В колонке {col} нет NaN значений")
    
    print(" Валидация преобразованных данных пройдена")
    return True


def validate_database_connection(credentials: Dict[str, str]) -> bool:

    print("\n" + "=" * 30)
    print("ВАЛИДАЦИЯ ПОДКЛЮЧЕНИЯ К БАЗЕ")
    print("=" * 30)
    
    conn = None
    try:
        conn = psycopg2.connect(**credentials)
        cursor = conn.cursor()
        
        # Проверяем версию PostgreSQL
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f" Подключение к PostgreSQL: {version.split(',')[0]}")
        
        # Проверяем текущую базу
        cursor.execute("SELECT current_database();")
        db_name = cursor.fetchone()[0]
        print(f"✓ Текущая база: {db_name}")
        
        cursor.close()
        conn.close()
        
        print(" Валидация подключения к БД пройдена")
        return True
        
    except Exception as e:
        print(f"Ошибка валидации подключения: {e}")
        if conn:
            conn.close()
        return False