import pandas as pd
from pymongo import MongoClient
import json

# Read the CSV file
df = pd.read_csv('data/F1Drivers_Dataset.csv')

df['Seasons'] = df['Seasons'].apply(lambda x: json.loads(x) if pd.notna(x) else [])
df['Championship Years'] = df['Championship Years'].apply(lambda x: json.loads(x) if isinstance(x, str) and x.strip() else [])
drivers_data = df.to_dict('records')

# Conexión MongoDB Atlas

connection_string = "mongodb+srv://paula:hANn9of1Jb8UzMZF@cluster0.obphjcn.mongodb.net/f1_database?retryWrites=true&w=majority"
client = MongoClient(connection_string)

# database y colección 
db = client['f1_database']
drivers_collection = db['drivers']

if drivers_data:
    result = drivers_collection.insert_many(drivers_data)
    print(f"Successfully inserted {len(result.inserted_ids)} documents")
else:
    print("No data to insert")

client.close()