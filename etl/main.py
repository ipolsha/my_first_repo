#!/usr/bin/env python3


import argparse
import sys
import os

# Добавляем путь для импорта модулей etl
sys.path.append(os.path.dirname(__file__))

from extract import extract_data, get_database_credentials
from transform import transform_data
from load import load_to_database, save_parquet, save_final_csv


def main():
    """Main ETL pipeline with CLI interface"""
    
    parser = argparse.ArgumentParser(
        description='ETL Pipeline для обработки данных из Google Sheets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python etl/main.py                          # Запуск с настройками по умолчанию
  python etl/main.py --table-name ivanova     # Указание имени таблицы
  python etl/main.py --max-rows 50            # Ограничение количества строк
  python etl/main.py --skip-db                # Без загрузки в БД
  python etl/main.py --skip-csv               # Без сохранения CSV
        """
    )
    
    parser.add_argument(
        '--table-name', 
        default='greskova',
        help='Название таблицы в БД (по умолчанию: greskova)'
    )
    parser.add_argument(
        '--max-rows',
        type=int,
        default=100,
        help='Максимальное количество строк для загрузки в БД (по умолчанию: 100)'
    )
    parser.add_argument(
        '--skip-db',
        action='store_true',
        help='Пропустить загрузку в базу данных'
    )
    parser.add_argument(
        '--skip-csv',
        action='store_true', 
        help='Пропустить сохранение CSV файла'
    )
    
    args = parser.parse_args()
    
    print("ЗАПУСК ETL ПАЙПЛАЙНА ИЗ GOOGLE SHEETS")
    print("=" * 60)
    print(f"Источник: Google Sheets")
    print(f"Таблица БД: {args.table_name}")
    print(f"Максимум строк в БД: {args.max_rows}")
    print(f"Пропуск БД: {args.skip_db}")
    print(f"Пропуск CSV: {args.skip_csv}")
    print("=" * 60)
    
    try:
        # Шаг 1: Extract - загрузка из Google Sheets
        print("\nЭТАП 1: ИЗВЛЕЧЕНИЕ ДАННЫХ")
        raw_df, raw_path = extract_data()
        
        # Шаг 2: Transform - преобразование данных
        print("\nЭТАП 2: ПРЕОБРАЗОВАНИЕ ДАННЫХ") 
        transformed_df = transform_data(raw_df)
        
        # Шаг 3: Load - загрузка в различные форматы
        print("\nЭТАП 3: ЗАГРУЗКА ДАННЫХ")
        
        # Загрузка в базу данных (если не пропущена)
        db_success = True
        if not args.skip_db:
            credentials = get_database_credentials()
            db_success = load_to_database(
                transformed_df, 
                credentials, 
                args.table_name,
                args.max_rows
            )
        
        # Сохранение в Parquet
        parquet_path = save_parquet(transformed_df)
        
        # Сохранение в CSV (если не пропущено)
        csv_path = None
        if not args.skip_csv:
            csv_path = save_final_csv(transformed_df)
        
        print("\nETL ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН!")
        print("=" * 50)
        print(f"✓ Сырые данные: {raw_path}")
        print(f"✓ Обработанные данные (Parquet): {parquet_path}")
        if csv_path:
            print(f"✓ Обработанные данные (CSV): {csv_path}")
        if not args.skip_db and db_success:
            print(f"✓ Данные в БД: таблица {args.table_name} ({args.max_rows} строк)")
        print(f"✓ Всего обработано строк: {len(transformed_df)}")
        print(f"✓ Всего колонок: {len(transformed_df.columns)}")
        
    except Exception as e:
        print(f"\nОШИБКА В ETL ПАЙПЛАЙНЕ: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()