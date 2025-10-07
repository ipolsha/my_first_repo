import pandas as pd
import requests
from bs4 import BeautifulSoup
import io

def parse_csv_from_google_sheets(sheet_id):
    """
    Парсинг CSV данных из Google Sheets с использованием Beautiful Soup
    """
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    

    response = requests.get(url)
    response.encoding = 'utf-8'
    
    # Используем Beautiful Soup для обработки текстовых данных
    soup = BeautifulSoup(response.text, 'html.parser')

    csv_data = soup.get_text()
    
    df = pd.read_csv(io.StringIO(csv_data))
    
    return df

# Парсинг первого датасета
print("Парсинг первого датасета...")
format = "csv"
id = "1scmkeENxadknow2rZ6H9LiG9m_BJmkBH"
df = parse_csv_from_google_sheets(id)
print("Первые 10 строк первого датасета:")
print(df.head(10))

# Парсинг второго датасета
print("\nПарсинг второго датасета...")
id1 = "1G6m-QoLgdWbOV3rSUBOaxn1cDQKkKk1H"
df1 = parse_csv_from_google_sheets(id1).rename(columns={"SMDBid": "SMDB_id"})
print("Первые 10 строк второго датасета:")
print(df1.head(10))

# Объединение данных
print("\nОбъединение данных...")
data = pd.merge(df, df1, on="SMDB_id")

# Удаление ненужных столбцов
data = data.drop(
    columns=[
        "Melting point (°C)",
        "Reference Link",
        "Field1_links",
        "Trust",
        "Efficacy_y",
        "Field8_links",
        "Reference",
    ]
)

print("Информация о данных после объединения:")
print(data.info())

# Обработка пропущенных значений
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

data = data.dropna(subset=string_columns)
data = data.convert_dtypes()
print("\nИнформация о данных после удаления пропущенных значений:")
print(data.info())

# Преобразование данных
print("\nПреобразование данных...")
data["Concentration new"] = (
    data["Concentration"]
    .str.split(" ")
    .str[0]
    .str.extract(r"^(\d+\.?\d*)$", expand=False)
)
data["id_"] = (
    data["SMDB_id"].str.split("SM").str[1].str.extract(r"^(\d+\.?\d*)$", expand=False)
)
data["Duration after transfection new"] = (
    data["Duration after transfection"].str.split(" ").str[0]
)

# Удаление строк с пропущенными значениями в новых столбцах
data = data.dropna(subset=["Concentration new", "Duration after transfection new"])

# Преобразование типов данных
data["Concentration new"] = data["Concentration new"].astype(float)
data["id_"] = data["id_"].astype(int)
data["Duration after transfection new"] = data[
    "Duration after transfection new"
].astype(int)

# Удаление исходных столбцов
data = data.drop(columns=["SMDB_id", "Concentration", "Duration after transfection"])

print("Типы данных после преобразования:")
print(data.dtypes)

# Сохранение результата
data.to_csv("new_data.csv", index=False)
print("\nДатасет сохранен под именем new_data.csv")
print(f"Размер финального датасета: {data.shape}")
print("\nПервые 5 строк финального датасета:")
print(data.head())