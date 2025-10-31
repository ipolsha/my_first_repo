import pandas as pd
import os
from typing import Tuple
from .validate import validate_raw_data


def extract_data_from_google_sheets() -> pd.DataFrame:

    print("=" * 50)
    print("EXTRACT: Загрузка данных из Google Sheets")
    print("=" * 50)
    
    # Первый датасет
    format = "csv"
    id1 = "1scmkeENxadknow2rZ6H9LiG9m_BJmkBH"
    print("Загружаем первый датасет...")
    df1 = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{id1}/export?format={format}")
    print(f"✓ Загружено {len(df1)} строк из первого датасета")
    
    # Второй датасет
    id2 = "1G6m-QoLgdWbOV3rSUBOaxn1cQBKkKk1H"
    print("Загружаем второй датасет...")
    df2 = pd.read_csv(
        f"https://docs.google.com/spreadsheets/d/{id2}/export?format={format}"
    ).rename(columns={"SMDBid": "SMDB_id"})
    print(f"✓ Загружено {len(df2)} строк из второго датасета")
    
    # Объединяем датасеты
    print("Объединяем датасеты по SMDB_id...")
    merged_data = pd.merge(df1, df2, on="SMDB_id")
    print(f"✓ Объединенный датасет: {len(merged_data)} строк")
    
    return merged_data


def clean_raw_data(df: pd.DataFrame) -> pd.DataFrame:

    print("Очистка сырых данных...")
    
    # Удаляем ненужные колонки
    columns_to_drop = [
        "Melting point (°C)",
        "Reference Link", 
        "Field1_links",
        "Trust",
        "Efficacy_y",
        "Field8_links",
        "Reference",
    ]
    
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    cleaned_data = df.drop(columns=existing_columns_to_drop)
    print(f"✓ Удалены колонки: {existing_columns_to_drop}")
    
    # Удаляем строки с пропусками в важных колонках
    string_columns = [
        "Experiment used to check activity",
        "Target gene", 
        "Cell or Organism used",
        "Transfection method",
        "siRNA sense",
        "siRNA antisense", 
        "Modification sense",
        "Modification antisense",
        "Position sense",
        "Position antisense",
    ]
    
    existing_string_cols = [col for col in string_columns if col in cleaned_data.columns]
    before_count = len(cleaned_data)
    cleaned_data = cleaned_data.dropna(subset=existing_string_cols)
    after_count = len(cleaned_data)
    print(f"Удалено строк с пропусками: {before_count - after_count}")
    print(f"Осталось строк: {after_count}")
    
    # Оптимизируем типы данных
    cleaned_data = cleaned_data.convert_dtypes()
    print("Типы данных оптимизированы")
    
    return cleaned_data


def save_raw_data(df: pd.DataFrame) -> str:

    os.makedirs('data/raw', exist_ok=True)
    output_path = 'data/raw/raw_data.csv'
    df.to_csv(output_path, index=False)
    print(f"Сырые данные сохранены в {output_path}")
    return output_path


def extract_data() -> Tuple[pd.DataFrame, str]:

    # Извлекаем данные из Google Sheets
    raw_df = extract_data_from_google_sheets()
    
    # Очищаем данные
    cleaned_df = clean_raw_data(raw_df)
    
    # Валидируем сырые данные
    validate_raw_data(cleaned_df)
    
    # Сохраняем сырые данные
    output_path = save_raw_data(cleaned_df)
    
    return cleaned_df, output_path


def get_database_credentials():

    import sqlite3
    import os
    
    # Пробуем из переменных среды
    credentials = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME', 'homeworks')
    }
    
    if all(credentials.values()):
        print("Учетные данные получены из переменных среды")
        return credentials
    
    # Пробуем из creds.db
    try:
        conn = sqlite3.connect('creds.db')
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM access")
        row = cursor.fetchone()
        conn.close()
        
        if row:
            credentials = {
                'host': row[0],
                'port': row[1],
                'user': row[2],
                'password': row[3],
                'database': 'homeworks'
            }
            print("Учетные данные получены из creds.db")
            return credentials
    except Exception as e:
        print(f"Не удалось получить данные из creds.db: {e}")
    
    raise ValueError("Не удалось получить учетные данные БД")