import boto3, json, decimal, time
import config as cfg

def create_movie_table(dynamo_db_client, table_name):
    if not find_table(dynamo_db_client, table_name):
        table = dynamo_db_client.create_table(
            TableName = table_name,
            KeySchema = 
            [
                {
                    'AttributeName': 'year',
                    'KeyType': 'HASH'  #Partition key
                },
                {
                    'AttributeName': 'title',
                    'KeyType': 'RANGE'  #Sort key
                },
            ],
            AttributeDefinitions = 
            [
                {
                    'AttributeName': 'year',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'title',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput = 
            {
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        print("Table '" + table_name + "' successfully created")
    else:
        print("Table '" + table_name + "' already exists")

def find_table(dynamo_db_client, table_name):
    table_names = dynamo_db_client.list_tables()['TableNames']
    return table_name in table_names

def load_movie_data(table_name, path_to_json_file):
    dynamo_db = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamo_db.Table(table_name)
    
    with open(path_to_json_file) as json_file:
        movies = json.load(json_file, parse_float=decimal.Decimal)
        for movie in movies:
            year = int(movie['year'])
            title = movie['title']
            info = movie['info']

            print("Adding movie:", year, title)

            table.put_item(
                Item = {
                    'year': year,
                    'title': title,
                    'info': info,
                }
            )

if __name__ == "__main__":
    start_time = time.time()
    dynamo_db_client = boto3.client('dynamodb', 'us-east-1')
    create_movie_table(dynamo_db_client, cfg.task_two['table_name'])
    while 1:
        table_status = dynamo_db_client.describe_table(TableName=cfg.task_two['table_name'])['Table']['TableStatus']
        if table_status == "ACTIVE":
            break

    load_movie_data(cfg.task_two['table_name'], './moviedata.json')
    print(str(time.time() - start_time) + " seconds.")