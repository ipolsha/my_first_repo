import pandas as pd
import os
from typing import Tuple
from .validate import validate_transformed_data


def transform_data(df: pd.DataFrame) -> pd.DataFrame:

    print("\n" + "=" * 50)
    print("TRANSFORM: Преобразование данных")
    print("=" * 50)
    
    # Создаем копию чтобы не менять оригинал
    transformed_df = df.copy()
    
    print("Исходные типы данных:")
    for col, dtype in transformed_df.dtypes.items():
        print(f"  - {col}: {dtype}")
    
    # Создаем новую колонку Concentration new
    print("\nСоздаем колонку Concentration new...")
    transformed_df["Concentration new"] = (
        transformed_df["Concentration"]
        .str.split(" ")
        .str[0]
        .str.extract(r"^(\d+\.?\d*)$", expand=False)
    )
    print("Колонка Concentration new создана")
    
    # Создаем колонку id_ из SMDB_id
    print("Создаем колонку id_...")
    transformed_df["id_"] = (
        transformed_df["SMDB_id"].str.split("SM").str[1].str.extract(r"^(\d+\.?\d*)$", expand=False)
    )
    print("Колонка id_ создана")
    
    # Создаем колонку Duration after transfection new
    print("Создаем колонку Duration after transfection new...")
    transformed_df["Duration after transfection new"] = (
        transformed_df["Duration after transfection"].str.split(" ").str[0]
    )
    print("Колонка Duration after transfection new создана")
    
    # Удаляем строки с пропусками в новых числовых колонках
    before_count = len(transformed_df)
    transformed_df = transformed_df.dropna(subset=["Concentration new", "Duration after transfection new"])
    after_count = len(transformed_df)
    print(f"Удалено строк с пропусками в числовых колонках: {before_count - after_count}")
    
    # Преобразуем типы данных
    print("\nПреобразуем типы данных...")
    
    # Concentration new -> float
    transformed_df["Concentration new"] = transformed_df["Concentration new"].astype(float)
    print("Concentration new преобразован в float")
    
    # id_ -> int
    transformed_df["id_"] = transformed_df["id_"].astype(int)
    print("id_ преобразован в int")
    
    # Duration after transfection new -> int
    transformed_df["Duration after transfection new"] = transformed_df[
        "Duration after transfection new"
    ].astype(int)
    print("Duration after transfection new преобразован в int")
    
    # Удаляем старые колонки
    columns_to_drop = ["SMDB_id", "Concentration", "Duration after transfection"]
    existing_columns_to_drop = [col for col in columns_to_drop if col in transformed_df.columns]
    transformed_df = transformed_df.drop(columns=existing_columns_to_drop)
    print(f"Удалены старые колонки: {existing_columns_to_drop}")
    
    print("\nТипы данных после преобразования:")
    for col, dtype in transformed_df.dtypes.items():
        print(f"  - {col}: {dtype}")
    
    # Валидация преобразованных данных
    validate_transformed_data(transformed_df)
    
    return transformed_df


def save_transformed_data(df: pd.DataFrame) -> str:

    output_path = 'new_data.csv'
    df.to_csv(output_path, index=False)
    print(f"Преобразованные данные сохранены в {output_path}")
    return output_path


def prepare_for_database(df: pd.DataFrame, table_name: str) -> str:

    # Сопоставление типов Pandas -> PostgreSQL
    type_mapping = {
        'int64': 'INTEGER',
        'int32': 'INTEGER', 
        'float64': 'DOUBLE PRECISION',
        'float32': 'REAL',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'object': 'TEXT'
    }
    
    columns_sql = []
    for column, dtype in df.dtypes.items():
        pg_type = type_mapping.get(str(dtype), 'TEXT')
        # Экранируем названия колонок если нужно
        safe_column = f'"{column}"' if ' ' in column or '-' in column else column
        columns_sql.append(f"{safe_column} {pg_type}")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(columns_sql)}
    );
    """
    
    print(f"Подготовлен SQL для таблицы {table_name}")
    return create_table_sql