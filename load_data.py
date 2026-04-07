import csv
import pymongo

CONN = "mongodb+srv://hasan2002618_db_user:lXKrTNS5od8Y6MId@cluster0.napsu7a.mongodb.net/"
CSV_FILE = "C:/Users/Hasan/Downloads/Electric_Vehicle_Population_Data.csv" 
BATCH_SIZE = 500   # num documents inserted per batch

client    = pymongo.MongoClient(CONN)
db        = client["ev_db"]
collection = db["vehicles"]

def load_data():
    batch = []
    total = 0

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            batch.append(row)

            # Once the batch is full, insert and reset
            if len(batch) == BATCH_SIZE:
                collection.insert_many(batch)
                total += len(batch)
                print(f"Inserted {total} records so far...")
                batch = []

        # Insert any remaining records that didn't fill a full batch
        if batch:
            collection.insert_many(batch)
            total += len(batch)

    print(f"Done! Total records inserted: {total}")

if __name__ == "__main__":
    load_data()