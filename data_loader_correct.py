import pandas as pd
df = pd.read_csv('https://docs.google.com/spreadsheets/d/1scmkeENxadknow2rZ6H9LiG9m_BJmkBH/export?format=csv')
print(df.head(10))
df = pd.read_csv('https://docs.google.com/spreadsheets/d/1G6m-QoLgdWbOV3rSUBOaxn1cDQKkKk1H/export?format=csv')
print(df.head(10))