from pymongo import MongoClient


def connect_to_atlas(username="analytics", password="salony7808335456", db="mflix", collection="initial_movies"):
    """connection to mongo attlas"""
    client = MongoClient("mongodb+srv://analytics:salony7808335456@cluster0.gffbu.mongodb.net/mflix"
                         "?retryWrites=true&w"
                         "=majority")
    db = client['mfilx']
    collection = db["initial_movies"]
    return collection
