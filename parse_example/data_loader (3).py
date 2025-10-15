import pandas as pd

format = "csv"
id = "1scmkeENxadknow2rZ6H9LiG9m_BJmkBH"
df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{id}/export?format={format}")
df.head(10)
id1 = "1G6m-QoLgdWbOV3rSUBOaxn1cDQKkKk1H"
df1 = pd.read_csv(
    f"https://docs.google.com/spreadsheets/d/{id1}/export?format={format}"
).rename(columns={"SMDBid": "SMDB_id"})
df1.head(10)

data = pd.merge(df, df1, on="SMDB_id")
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

print(data.info())

string = [
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
data = data.dropna(subset=string)
data = data.convert_dtypes()
print(data.info())

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

data = data.dropna(subset=["Concentration new", "Duration after transfection new"])
data["Concentration new"] = data["Concentration new"].astype(float)
data["id_"] = data["id_"].astype(int)
data["Duration after transfection new"] = data[
    "Duration after transfection new"
].astype(int)
data = data.drop(columns=["SMDB_id", "Concentration", "Duration after transfection"])
print("Типы данных после преобразования:")
print(data.dtypes)

data.to_csv("new_data.csv")
print("Датасет сохранен под именем new_data.csv")

data.to_parquet("new_data.parquet", index=False)
print("Датасет сохранен под именем new_data.parquet")
