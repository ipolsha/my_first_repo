import pandas as pd

raw_data = pd.read_excel(
    "C:\MyPythonProjects\my_first_repo\data\siRNAmod_db_parsed_concs.xlsx"
)
print(raw_data.head(10))
