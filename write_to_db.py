import os
import sqlite3
import psycopg2
import pandas as pd
from typing import Dict
from dotenv import load_dotenv


def extract_credentials_from_sqlite() -> Dict[str, str]:
    """Извлекаем учетные данные из SQLite базы и сохраняем в .env"""

    print("=" * 50)
    print("ИЗВЛЕЧЕНИЕ УЧЕТНЫХ ДАННЫХ ИЗ CREDS.DB")
    print("=" * 50)

    try:
        # Проверяем существование файла creds.db
        if not os.path.exists("creds.db"):
            raise FileNotFoundError("Файл creds.db не найден в текущей директории")

        print(" Файл creds.db найден")

        # Подключаемся к SQLite базе
        conn = sqlite3.connect("creds.db")
        cursor = conn.cursor()

        # Проверяем какие таблицы есть
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f" Таблицы в базе: {tables}")

        # Получаем данные из таблицы access
        cursor.execute("SELECT * FROM access")
        row = cursor.fetchone()

        if not row:
            raise ValueError("В таблице access нет данных")

        print(f" Получены данные из таблицы access: {len(row)} полей")

        # Извлекаем данные (предполагаемая структура: url, port, user, password)
        host, port, user, password = row[0], row[1], row[2], row[3]
        database = "homeworks"  # из условия задания

        conn.close()

        # Сохраняем в .env файл
        env_path = ".env"
        with open(env_path, "w") as f:
            f.write(f"DB_HOST={host}\n")
            f.write(f"DB_PORT={port}\n")
            f.write(f"DB_USER={user}\n")
            f.write(f"DB_PASSWORD={password}\n")
            f.write(f"DB_NAME={database}\n")

        print(f"✓ Создан файл: {env_path}")
        print(f"✓ Полный путь: {os.path.abspath(env_path)}")

        # Проверяем что файл создался
        if os.path.exists(env_path):
            print("✓ Файл .env успешно создан и проверен")
        else:
            print(" Файл .env не создался!")

        return {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
        }

    except sqlite3.Error as e:
        raise ValueError(f"Ошибка чтения из creds.db: {e}")
    except Exception as e:
        raise ValueError(f"Ошибка извлечения данных: {e}")


extract_credentials_from_sqlite()


import os
import sqlite3
import psycopg2
import pandas as pd
from typing import Dict
from dotenv import load_dotenv


def get_credentials() -> Dict[str, str]:
    """Получаем учетные данные: сначала из .env, если нет - извлекаем из creds.db"""

    print("=" * 50)
    print("ПОЛУЧЕНИЕ УЧЕТНЫХ ДАННЫХ")
    print("=" * 50)

    # Загружаем переменные из .env файла
    load_dotenv()

    # Проверяем, есть ли уже переменные в .env
    env_credentials = {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME", "homeworks"),
    }

    # Если .env файл уже существует и содержит все данные
    if all(env_credentials.values()):
        print("Используем учетные данные из .env файла")

        # Показываем данные (без пароля)
        safe_creds = env_credentials.copy()
        safe_creds["password"] = "***"
        print("Учетные данные:")
        for key, value in safe_creds.items():
            print(f"  {key}: {value}")

        return env_credentials
    else:
        # Если .env файла нет или он неполный, извлекаем из creds.db
        print(".env файл не найден или неполный, извлекаем данные из creds.db")
        return extract_credentials_from_sqlite()


def load_and_prepare_dataset(file_path: str) -> pd.DataFrame:
    """Загружаем и подготавливаем датасет"""

    print("\n" + "=" * 50)
    print("ЗАГРУЗКА И ПОДГОТОВКА ДАТАСЕТА")
    print("=" * 50)

    # Загружаем данные
    df = pd.read_csv(file_path)
    print(f"Загружено {len(df)} строк")
    print(f"Исходные колонки: {list(df.columns)}")

    # Обрабатываем числовые колонки - убираем единицы измерения
    numeric_columns = ["Concentration new", "Duration after transfection new"]

    for col in numeric_columns:
        if col in df.columns:
            print(f"\nОбрабатываем колонку '{col}':")

            # Показываем примеры значений до обработки
            sample_values = df[col].head(3).tolist()
            print(f"  Примеры значений до обработки: {sample_values}")

            # Преобразуем в числовой формат
            df[col] = pd.to_numeric(df[col], errors="coerce")

            # Проверяем результат
            print(f"  Тип данных после обработки: {df[col].dtype}")
            print(
                f"  Успешно преобразовано: {df[col].notna().sum()}/{len(df)} значений"
            )

    print(f"\n Обработанные колонки: {list(df.columns)}")
    print("Типы данных после обработки:")
    for col, dtype in df.dtypes.items():
        print(f"  - {col}: {dtype}")

    print("\nПервые 3 строки обработанного датасета:")
    print(df.head(3))

    return df


def create_table_sql(df: pd.DataFrame, table_name: str) -> str:
    """Создаем SQL для создания таблицы с правильными типами данных"""

    print(f"\nСоздаем структуру для таблицы '{table_name}'...")

    # Сопоставление типов Pandas -> PostgreSQL
    type_mapping = {
        "int64": "INTEGER",
        "int32": "INTEGER",
        "float64": "DOUBLE PRECISION",
        "float32": "REAL",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
        "object": "TEXT",
    }

    columns_sql = []
    for column, dtype in df.dtypes.items():
        pg_type = type_mapping.get(str(dtype), "TEXT")
        # Экранируем названия колонок, если они содержат специальные символы
        safe_column = f'"{column}"' if " " in column or "-" in column else column
        columns_sql.append(f"{safe_column} {pg_type}")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(columns_sql)}
    );
    """

    print(" Структура таблицы определена:")
    for col in columns_sql:
        print(f"  - {col}")

    return create_table_sql


def write_to_postgres(
    credentials: Dict[str, str], df: pd.DataFrame, table_name: str
) -> bool:
    """Записываем данные в PostgreSQL"""

    print("\n" + "=" * 50)
    print(f"ЗАПИСЬ ДАННЫХ В ТАБЛИЦУ {table_name}")
    print("=" * 50)

    if df is None or len(df) == 0:
        print(" Нет данных для записи")
        return False

    conn = None
    try:
        # Подключаемся к PostgreSQL
        conn = psycopg2.connect(**credentials)
        cursor = conn.cursor()
        print(" Подключение к PostgreSQL установлено")

        # Создаем таблицу
        create_sql = create_table_sql(df, table_name)
        cursor.execute(create_sql)
        print(" Таблица создана/проверена")

        # Очищаем таблицу (если она уже существовала)
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        print(" Таблица очищена")

        # Записываем данные (максимум 100 строк)
        data_to_insert = df.head(100)
        print(f" Записываем {len(data_to_insert)} строк...")

        # Подготавливаем SQL для вставки
        columns = ", ".join(
            [f'"{col}"' if " " in col or "-" in col else col for col in df.columns]
        )
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Вставляем данные построчно
        for index, row in data_to_insert.iterrows():
            cursor.execute(insert_sql, tuple(row))

        # Подтверждаем изменения
        conn.commit()
        print(" Данные записаны в базу")

        # Проверяем запись
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_in_db = cursor.fetchone()[0]
        print(f" В таблице теперь {count_in_db} строк")

        # Показываем пример записанных данных
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
        sample_data = cursor.fetchall()

        print(f"\nПример записанных данных (первые 3 строки):")
        for i, row in enumerate(sample_data, 1):
            print(f"  Строка {i}: {row}")

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


def verify_table_creation(credentials: Dict[str, str], table_name: str) -> bool:
    """Проверяем, что таблица создана и доступна"""

    print("\n" + "=" * 40)
    print("ПРОВЕРКА СОЗДАНИЯ ТАБЛИЦЫ")
    print("=" * 40)

    conn = None
    try:
        conn = psycopg2.connect(**credentials)
        cursor = conn.cursor()

        # Проверяем структуру созданной таблицы
        cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s 
            ORDER BY ordinal_position
        """,
            (table_name,),
        )

        columns = cursor.fetchall()

        if columns:
            print(f" Таблица '{table_name}' создана успешно!")
            print(f" Структура таблицы ({len(columns)} колонок):")
            for col_name, data_type in columns:
                print(f"  - {col_name} ({data_type})")
        else:
            print(f" Таблица '{table_name}' не найдена")
            return False

        # Проверяем количество данных
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f" Количество строк в таблице: {row_count}")

        return True

    except Exception as e:
        print(f" Ошибка при проверке таблицы: {e}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()


def main():
    """Основная функция выполнения ДЗ"""

    print(" ВЫПОЛНЕНИЕ ДОМАШНЕГО ЗАДАНИЯ №6")
    print("=" * 60)

    try:

        credentials = get_credentials()

        dataset_path = "new_data.csv"
        df = load_and_prepare_dataset(dataset_path)

        table_name = "greskova"

        success = write_to_postgres(credentials, df, table_name)

        if success:

            verification = verify_table_creation(credentials, table_name)

            if verification:
                print("\n ЗАДАНИЕ ВЫПОЛНЕНО")
                print("=" * 50)
                print(f" Учетные данные извлечены из creds.db в .env")
                print(f" Таблица '{table_name}' создана в схеме public")
                print(f" Данные записаны (максимум 100 строк)")
                print(f" Числовые колонки обработаны правильно")
            else:
                print("\n Проблема с проверкой таблицы")
        else:
            print("\n Не удалось записать данные в базу")

    except Exception as e:
        print(f" Ошибка: {e}")


if __name__ == "__main__":
    main()
