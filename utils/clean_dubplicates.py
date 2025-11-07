import pandas as pd

file_name="general.blacklist.csv"
# Загружаем CSV
df = pd.read_csv(file_name)

# Удаляем дубликаты по колонке 'symbol', оставляя первую встречу
df_cleaned = df.drop_duplicates(subset="symbol", keep="first")

# Сохраняем очищенный файл
df_cleaned.to_csv (file_name + ".cleaned.csv", index=False)
