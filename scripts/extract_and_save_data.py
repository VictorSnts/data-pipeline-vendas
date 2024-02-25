from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests


def connect_mongo(uri):
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client


def create_connect_db(client, db_name):
    db = client[db_name]
    print(f"Connected to database {db_name}.")
    return db


def create_connect_collection(db, collection_name):
    collection = db[collection_name]
    print(f"Connected to collection {collection_name}.")
    return collection


def extract_api_data(url):
    response = requests.get(url)
    num_rec = len(response.json())
    print(f"Data extracted from the API {url}. Number of records: {num_rec}")
    return response.json()


def insert_data(collection, data):
    docs = collection.insert_many(data)
    num_docs = len(docs.inserted_ids)
    print(f"Data saved in the MongoDB database. Number of docs: {num_docs}")
    return num_docs


if __name__ == "__main__":
    uri = "mongodb+srv://victorsantos:12345@cluster-pipeline.wakdkz3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-Pipeline"
    db_name = "db_produtos"
    collection_name = "produtos"
    url_api = "https://labdados.com/produtos"

    client = connect_mongo(uri)
    db = create_connect_db(client, db_name)
    collection = create_connect_collection(db, collection_name)
    data_api = extract_api_data(url_api)
    insert_data(collection, data_api)
    client.close()

