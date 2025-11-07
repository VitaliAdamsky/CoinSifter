import pandas as pd

# Load the CSV file
df = pd.read_csv("coins_export.csv")

# Count the number of rows
count = len(df)

print(f"Total number of elements: {count}")
