# my_first_repo
**Инжиниринг данных 2025**

## 📁 Структура проекта


my_first_repo/
├── etl/ # ETL пайплайн (ДЗ №8)
│ ├── init.py
│ ├── extract.py # Извлечение данных
│ ├── transform.py # Преобразование данных
│ ├── load.py # Загрузка данных
│ ├── validate.py # Валидация
│ └── main.py # CLI интерфейс
├── api_example/ 
│ ├── api_parser.py
├── EDA/ 
│ ├── EDA.ipynb #визуализация и оценка данных
├── write_to_db.py # Скрипт для ДЗ №6 (остался для проверки)
├── package-list.txt # Зависимости Conda
├── .gitignore # Игнорируемые файлы
└── README.md # Документация


## Датасет

siRNAmod database взять за основу. Содержит информацию о химически модифицированных малых интерферирующих РНК. Датасет подходит для дальнейшей работы по разработке прогностических моделей


## Быстрый старт

### Установка зависимостей

**С помощью Conda:**

conda create -n my_env python=3.13.7
conda activate my_env
conda install --file package-list.txt


## Запуск ETL пайплайна
# Базовый запуск
python etl/main.py

# С указанием таблицы
python etl/main.py --table-name greskova

# Ограничение количества строк
python etl/main.py --max-rows 50

# Без загрузки в БД
python etl/main.py --skip-db

Перед запуском необходимо подгрузить в репозиторий файл creds.db

##  Выходные данные
После выполнения ETL пайплайна создаются:

data/raw/raw_data.csv - сырые данные

data/processed/processed_data.parquet - обработанные данные

new_data.csv - финальные данные в CSV

new_data.parquet - финальные данные в Parquet

Таблица в PostgreSQL


Данные лежат тут:

https://drive.google.com/drive/folders/1ngSD7Uz3NhaQJdDPdo7BRXDT-MzvHG6J?usp=sharing
<img width="1280" height="397" alt="image" src="https://github.com/user-attachments/assets/5d73f9e6-8a2a-4faf-96d8-d0f732551640" />


Мои данные с таким типом:
<img width="1280" height="667" alt="image" src="https://github.com/user-attachments/assets/e39b68e2-da41-42b9-93fb-4070801ca3b5" />


