from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

app = Flask(__name__)

CONN = "mongodb+srv://hasan2002618_db_user:lXKrTNS5od8Y6MId@cluster0.napsu7a.mongodb.net/"
client = MongoClient(CONN)
collection = client["ev_db"]["vehicles"]


@app.route("/insert-fast", methods=["POST"])
def insert_fast():
    record = request.get_json()
    fast_collection = collection.with_options(
        write_concern=WriteConcern(1))
    result = fast_collection.insert_one(record)
    return jsonify({"inserted_id": str(result.inserted_id)})


@app.route("/insert-safe", methods=["POST"])
def insert_safe():
    record = request.get_json()
    safe_collection = collection.with_options(
        write_concern=WriteConcern("majority"))
    result = safe_collection.insert_one(record)
    return jsonify({"inserted_id": str(result.inserted_id)})


@app.route("/count-tesla-primary", methods=["GET"])
def count_tesla_primary():
    primary_collection = collection.with_options(
        read_preference=ReadPreference.PRIMARY)
    count = primary_collection.count_documents({"Make": "TESLA"})
    return jsonify({"count": count})


@app.route("/count-bmw-secondary", methods=["GET"])
def count_bmw_secondary():
    secondary_collection = collection.with_options(
        read_preference=ReadPreference.SECONDARY_PREFERRED)
    count = secondary_collection.count_documents({"Make": "BMW"})
    return jsonify({"count": count})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
