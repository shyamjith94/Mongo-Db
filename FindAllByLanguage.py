from ConnectionMongoAtlas import connect_to_atlas
from pprint import pprint
from bson.json_util import dumps
from bson.json_util import loads

collection = connect_to_atlas(collection='movies_scratch')
filters = {
    'languages': {'$all': ['English']}
}  # all the language array have English

filters = {
    'languages.0': 'English'
}  # language array first element should be 'English

#  field that are include in title and language
projection = {
    '_id': 0,
    'title': 1,
    'languages': 1
}

result = collection.find(filters, projection)
pprint(loads(dumps(result)))
