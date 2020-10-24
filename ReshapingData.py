"""Reshaping filed to array mongo db collection"""
from ConnectionMongoAtlas import connect_to_atlas
import pprint
from bson.json_util import dumps
from bson.json_util import loads

collection = connect_to_atlas(collection="initial_movies")
pip_line = [
    {
        '$limit': 100
    },
    {
        '$project': {
            'title': 1,  # simply access the field from collection 1, 0 exclude fields
            'year': 1,
            'directors': {'$split': ["$director", ","]},
            'cast': {'$split': ["$cast", ","]},
            'writers': {'$split': ['$writer', ","]},
            'genres': {'$split': ["$genre", ","]},
            'languages': {'$split': ['$language', ","]},
            'countries': {'$split': ['$country', ","]},
            'plot': 1,
            'fullPlot': "$fullplot",
            'rated': "$rating",
            'released': 1,
            'runtime': 1,
            'poster': 1,
            'imdb': {
                'id': "$imdbID",
                'rating': "$imdbRating",
                'votes': "$imdbVotes"
            },
            'metacritic': 1,
            'award': 1,
            'type': 1,
            'lastUpdated': "$lastupdated",
        }
    }
]
try:
    result = collection.aggregate(list(pip_line))
    pprint.pprint(loads(dumps(result)))
except Exception as e:
    print(e.__str__())
