import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load MongoDB URL from .env
load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")

# Connect to MongoDB
client = MongoClient(MONGO_DB_URL)
db = client["general"]
collection = db["blacklist"]

# Your input list of symbols
symbols = [
  "REZ",
  "OM",
  "KAS",
  "DEEP",
  "FLOCK",
  "WCT",
  "SNX",
  "SOL",
  "AVA",
  "PYTH",
  "ACH",
  "DOGE",
  "1000LUNC",
  "HYPE",
  "DEXE",
  "VTHO",
  "SUN",
  "ORDER",
   
]

# Remove duplicates from input
unique_symbols = list(set(symbols))

# Logging counters
total_received = len(symbols)
unique_input = len(unique_symbols)
already_in_db = 0
added_to_db = 0

print(f"\n📦 Received {total_received} symbols.")
print(f"🧹 After removing duplicates: {unique_input} unique symbols.\n")

# Insert only symbols not already in the collection
for symbol in unique_symbols:
    if collection.find_one({"symbol": symbol}):
        already_in_db += 1
        print(f"⚠️ Skipped (already in DB): {symbol}")
    else:
        collection.insert_one({"symbol": symbol})
        added_to_db += 1
        print(f"✅ Added: {symbol}")

# Final summary
print("\n📊 Summary:")
print(f"🔢 Total received: {total_received}")
print(f"🧺 Unique input: {unique_input}")
print(f"🗃️ Already in DB: {already_in_db}")
print(f"🆕 Newly added: {added_to_db}\n")
