import json, decimal, pymongo, time
import config as cfg
from azure.cosmos.cosmos_client import CosmosClient

def create_database(cosmos_client):
    database = cosmos_client[DB_NAME]
    return database

def create_movie_container(database):
    container = database[CONTAINER_NAME]
    return container

def load_movie_data(container, path_to_json_file):
    if container.estimated_document_count() == 0:
        with open(path_to_json_file) as json_file:
            movies = json.load(json_file, parse_float=decimal.Decimal)
            for movie in movies:
                year = int(movie['year'])
                title = movie['title']
                info = movie['info']

                if 'rating' in info.keys():
                    info['rating'] = str(info['rating'])
                else:
                    info['rating'] = "N/A"

                print("Adding movie:", year, title)
                item = {
                    'year': year,
                    'title': title,
                    'info': info
                }
                container.insert_one(item)
    else:
        print("Error: Table '" + container.name + "' not empty.")

if __name__ == "__main__":
    start_time = time.time()
    DB_NAME = cfg.task_two['database_name']
    CONTAINER_NAME = cfg.task_two['table_name']
    CONNECTION_STRING = cfg.task_two['connection_string']

    cosmos_client = pymongo.MongoClient(CONNECTION_STRING)
    database = create_database(cosmos_client)
    movie_container = create_movie_container(database)
    load_movie_data(movie_container, "./moviedata.json")
    print(str(time.time() - start_time) + " seconds.")
