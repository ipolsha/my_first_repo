import pandas as pd
import psycopg2
import os
from typing import Dict
from .validate import validate_database_connection


def load_to_database(df: pd.DataFrame, credentials: Dict[str, str], table_name: str, max_rows: int = 100) -> bool:

    print("\n" + "=" * 50)
    print(f"LOAD: Загрузка в базу данных (таблица: {table_name})")
    print("=" * 50)
    
    if df is None or len(df) == 0:
        print(" Нет данных для загрузки")
        return False
    
    # Валидация подключения к БД
    validate_database_connection(credentials)
    
    conn = None
    try:
        # Подключаемся к PostgreSQL
        conn = psycopg2.connect(**credentials)
        cursor = conn.cursor()
        print(" Подключение к PostgreSQL установлено")
        
        # Создаем таблицу
        from .transform import prepare_for_database
        create_sql = prepare_for_database(df, table_name)
        cursor.execute(create_sql)
        print(" Таблица создана/проверена")
        
        # Очищаем таблицу
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        print(" Таблица очищена")
        
        # Записываем данные (максимум max_rows строк)
        data_to_insert = df.head(max_rows)
        print(f" Загружаем {len(data_to_insert)} строк (максимум {max_rows})...")
        
        # Подготавливаем SQL для вставки
        columns = ', '.join([f'"{col}"' if ' ' in col or '-' in col else col for col in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Вставляем данные построчно
        inserted_count = 0
        for index, row in data_to_insert.iterrows():
            try:
                cursor.execute(insert_sql, tuple(row))
                inserted_count += 1
            except Exception as e:
                print(f" Ошибка вставки строки {index}: {e}")
        
        # Подтверждаем изменения
        conn.commit()
        print(f" Данные записаны в базу ({inserted_count} строк)")
        
        # Проверяем запись
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_in_db = cursor.fetchone()[0]
        print(f" В таблице теперь {count_in_db} строк")
        
        # Показываем пример данных
        if count_in_db > 0:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 2")
            sample_data = cursor.fetchall()
            print(f"\nПример записанных данных (первые 2 строки):")
            for i, row in enumerate(sample_data, 1):
                print(f"  Строка {i}: {row[:3]}...")  # Показываем первые 3 значения
        
        return True
        
    except psycopg2.Error as e:
        print(f" Ошибка PostgreSQL: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        print(f" Общая ошибка: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()
            print(" Соединение с PostgreSQL закрыто")


def save_parquet(df: pd.DataFrame) -> str:

    print("\n" + "=" * 30)
    print("СОХРАНЕНИЕ PARQUET")
    print("=" * 30)
    
    # Создаем директорию data/processed если нет
    os.makedirs('data/processed', exist_ok=True)
    
    # Сохраняем в Parquet
    output_path = 'data/processed/processed_data.parquet'
    df.to_parquet(output_path, index=False)
    print(f" Данные сохранены в {output_path}")
    print(f" Размер файла: {os.path.getsize(output_path)} байт")
    
    # Также сохраняем в корень для обратной совместимости
    root_path = 'new_data.parquet'
    df.to_parquet(root_path, index=False)
    print(f" Данные также сохранены в {root_path}")
    
    return output_path


def save_final_csv(df: pd.DataFrame) -> str:

    output_path = 'new_data.csv'
    df.to_csv(output_path, index=False)
    print(f" Финальные данные сохранены в {output_path}")
    return output_path