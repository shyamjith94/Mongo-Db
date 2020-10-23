from ConnectionMongoAtlas import connect_to_atlas
from pprint import pprint
from datetime import datetime
from pymongo import UpdateOne
from bson.json_util import dumps
from bson.json_util import loads
import re

collection = connect_to_atlas()
runtime_pat = re.compile(r'([0-9]+) min')

update = []
batch_size = 1000
count = 0

for movie in collection.find({}):
    fields_to_set = {}
    fields_to_unset = {}

    for k, v in movie.copy().items():
        if v == "" or k == [""]:
            del movie[k]
            fields_to_unset[k] = ""
    if 'director' in movie:
        fields_to_unset['director'] = ""
        fields_to_set['directors'] = movie['director'].split(",")
    if 'cast' in movie and not isinstance(movie['cast'], list):
        # if not unset cast. we using same array name version in collection
        fields_to_set['cast'] = movie['cast'].split(",")
    if 'writer' in movie:
        fields_to_unset['writer'] = ""
        fields_to_set['writers'] = movie['writer'].split(",")
    if 'genre' in movie:
        fields_to_unset['genre'] = ""
        fields_to_set['genres'] = movie['genre'].split(",")
    if 'country' in movie:
        fields_to_unset['country'] = ""
        fields_to_set['countries'] = movie['country'].split(",")
    if 'fullplot' in movie:
        fields_to_unset['fullplot'] = ""
        fields_to_set['fullPlot'] = movie['fullplot']  # no need to split. have text data
    if 'rating' in movie:
        fields_to_unset['rating'] = ""
        fields_to_set['rated'] = movie['rating']
    # creating new new document for imdb
    imdb = {}
    if 'imdbID' in movie:
        fields_to_unset['imdbID'] = ""
        imdb['id'] = movie['imdbID']
    if 'imdbRating' in movie:
        fields_to_unset['imdbRating'] = ""
        imdb['imdbRating'] = movie['imdbRating']
    if 'imdbVotes' in movie:
        fields_to_unset['imdbVotes'] = ""
        imdb['imdbVotes'] = movie['imdbVotes']
    if imdb:
        fields_to_set['imdb'] = imdb
    # convert string to datetime
    if 'released' in movie:
        fields_to_set['released'] = datetime.strptime(movie['released'], "%Y-%m-%d")
    if 'lastUpdated' in movie:
        fields_to_set['lastUpdated'] = datetime.strptime(movie['lastUpdated'], "%Y-%m-%d %H:%M:%S")
    # eliminating string from runtime fields
    if 'runtime' in movie:
        m = runtime_pat.match(movie['runtime'])
        if m:
            fields_to_set['runtime'] = int(m.group(1))
    # creating new document for updating values
    update_doc = {}
    if fields_to_set:
        update_doc['$set'] = fields_to_set
    if fields_to_unset:
        update_doc['$unset'] = fields_to_unset

    update.append(UpdateOne({'_id': movie['_id']}, update_doc))
    count += 1
    if count == batch_size:
        collection.bulk_write(update)
        update = []
        count = 0
if update:
    collection.bulk_write(update)

# pprint(update_doc)
# pprint(movie['_id'])
# collection.update_one({'_id': movie['_id']}, update_doc)
