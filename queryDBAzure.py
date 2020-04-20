import decimal, pymongo, csv, json, time
import config as cfg
from prettytable import PrettyTable

def main_loop(movie_container):
    print("---------------------------")
    print("WELCOME TO COSMOSDB (Azure)")
    print("---------------------------", end="")

    while 1:
        print("\nQuery the Azure NOSQL (Mongo) Movie DB - Enter 'exit' to quit")
        user_input = input("Primary Key (Year)> Individual [ind] or Range [ran] ")
    
        try:   
            projection_attributes = ['year', 'title', 'info.rating']     
            primary_key_query = None
            sort_key_query = None
            optional_query = {}
            sort = None

            if user_input == "exit":
                exit()
            else:
                if user_input == "ind":
                    primary_key_query = input("Primary Key (Year)> Equal to ")
                elif user_input == "ran":
                    primary_key_query = input("Primary Key (Year)> in Range ")
                    if not is_valid_range_query(primary_key_query, False):
                        print("Error: Invalid range query. Please try again.")
                        continue

                user_input = input("Secondary Key (Title)> Individual [ind] or Range [ran] ")
                if user_input == "ind":
                    sort_key_query = input("Secondary Key (Title)> Equal to ")
                elif user_input == "ran":
                    sort_key_query = input("Secondary Key (Title)> in Range ")

                user_input = input("Rating> Less Than [lt] or Greater Than [gt] or Range [ran] ")
                if user_input == "lt" or user_input == "gt":
                    optional_query['value'] = input("Rating> Value ")
                    optional_query['type'] = user_input
                elif user_input == "ran":
                    optional_query['value'] = input("Rating> in Range ")
                    optional_query['type'] = user_input

                    if not is_valid_range_query(optional_query['value'], True):
                        print("Error: Invalid range query. Please try again.")
                        continue
                else:
                    optional_query = None

                start_time = time.time()
                json_response = execute_query(movie_container, primary_key_query, sort_key_query, optional_query)
                end_time = time.time()

                user_input = input("Attributes> Enter a comma separated list of attributes to include in result (optional) ")
                if user_input != "":
                    for attr in user_input.split(","):
                        attr = attr.strip()
                        if attr not in projection_attributes:
                            projection_attributes.append(attr)

                user_input = input("Sort> Primary Key (Year) [p], Secondary Key (Title) [s], or Other [o] ")
                field_name = ""
                other_field = False

                if user_input == "o":
                    field_name = input("Sort> Field Name is ")
                    other_field = True
                elif user_input == "p":
                    field_name = "year"
                elif user_input == "s":
                    field_name = "title"

                if user_input != "":
                    order = input("Sort> Ascending [asc] or Descending [desc] ")
                    reverse_order = False
                    
                    if order == "desc":
                        reverse_order = True
                        
                    json_response = sort_response(json_response, field_name, other_field, reverse_order)

                print(str(len(json_response)) + " results.")
                table = build_table(json_response, projection_attributes)
                print(table)

                user_input = input("Save> Save to csv [y/n]? ")
                if user_input == "y":
                    user_input = input("Save> Filename? ")
                    write_table_to_csv(table, user_input)

                print(str(end_time - start_time) + " seconds.")

        except ValueError as e:
            print("Error: Invalid value input. Please try again.")
            continue
        except Exception as e:
            print(e)
            continue

def is_valid_range_query(range_query, floating_point):
    range_query = range_query.split("-")
    if len(range_query) != 2:
        return False

    try:
        if floating_point:
            start_range = float(range_query[0])
            end_range = float(range_query[1])
        else:
            start_range = int(range_query[0])
            end_range = int(range_query[1])
    except ValueError as e:
        return False
        
    return True

def execute_query(movie_container, primary_key_query, sort_key_query, rating_key_query):
    query = {}
    response = None

    if primary_key_query:
        primary_key_query = primary_key_query.split("-")

        if len(primary_key_query) == 2:
            start_range = int(primary_key_query[0])
            end_range = int(primary_key_query[1])     
            query["year"] = {}
            query["year"]["$gte"] = start_range
            query["year"]["$lte"] = end_range
          
        elif len(primary_key_query) == 1:
            primary_key_value = int(primary_key_query[0])
            query["year"] = primary_key_value

    if sort_key_query:
        sort_key_query = sort_key_query.split("-")

        if len(sort_key_query) == 2:
            start_range = sort_key_query[0]
            end_range = sort_key_query[1]
            query["title"] = {}
            query["title"]["$gte"] = start_range
            query["title"]["$lte"] = end_range

        elif len(sort_key_query) == 1:
            query["title"] = sort_key_query[0]

    if rating_key_query:
        query["info.rating"] = {}

        if rating_key_query['type'] == 'ran':
            rating_key_query['value'] = rating_key_query['value'].split("-")
            start_range = rating_key_query['value'][0]
            end_range = rating_key_query['value'][1]
            query["info.rating"]["$lte"] = end_range
            query["info.rating"]["$gte"] = start_range
        else:
            rating_filter_value = rating_key_query['value']
            if rating_key_query['type'] == 'gt':
                query["info.rating"]["$gt"] = rating_filter_value
            else:
                query["info.rating"]["$lt"] = rating_filter_value

    response = list(movie_container.find(query))
    return response

def sort_response(json, field_name, other_field, reverse_order):
    if other_field:
        json.sort(key=lambda item: item['info'][field_name], reverse=reverse_order)
    else:
        json.sort(key=lambda item: item[field_name], reverse=reverse_order)
    return json

def build_table(json_response, column_names):
    output_table = PrettyTable()
    output_table.field_names = column_names

    for item in json_response:
        row_values = []
        for i in range(len(column_names)):
            splitted = column_names[i].split(".")    
            if splitted[0] == "info":
                if splitted[1] not in item['info'].keys():
                    item['info'][splitted[1]] = "N/A"
                row_values.append(item["info"][splitted[1]])
            else:
                if column_names[i] not in item.keys():
                    item[column_names[i]] = "N/A"
                row_values.append(item[column_names[i]])

        output_table.add_row(row_values)
    return output_table

def write_table_to_csv(table, filename):
    query_file = open(filename + '.csv', 'w') 
    csv_writer = csv.writer(query_file) 
    csv_writer.writerow(table._field_names)

    for row in table._rows:
        csv_writer.writerow(row) 

    query_file.close()

if __name__ == "__main__":
    DB_NAME = cfg.task_two['database_name']
    CONTAINER_NAME = cfg.task_two['table_name']
    CONNECTION_STRING = cfg.task_two['connection_string']

    cosmos_client = pymongo.MongoClient(CONNECTION_STRING)
    database = cosmos_client[DB_NAME]
    movie_container = database.get_collection(CONTAINER_NAME)
    main_loop(movie_container)
