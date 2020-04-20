import boto3, decimal, csv, time
from prettytable import PrettyTable
from boto3.dynamodb.conditions import Key, Attr

def main_loop(dynamo_client):
    print("-------------------------")
    print("WELCOME TO DYNAMODB (AWS)")
    print("-------------------------", end="")

    while 1:
        print("\nQuery the NOSQL Movie DB - Enter 'exit' to quit")
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
                    if not is_valid_range_query(primary_key_query):
                        print("Error: Invalid range query. Please try again.")
                        continue

                user_input = input("Secondary Key (Title)> Individual [ind] or Range [ran] ")
                if user_input == "ind":
                    sort_key_query = input("Secondary Key (Title)> Equal to ")
                elif user_input == "ran":
                    sort_key_query = input("Secondary Key (Title)> in Range ")

                user_input = input("Rating> Less Than [lt] or Greater Than [gt] ")
                if user_input == "lt" or user_input == "gt":
                    optional_query['value'] = float(input("Rating> Value "))
                    optional_query['type'] = user_input
                else:
                    optional_query = None

                start_time = time.time()
                json_response = execute_query(primary_key_query, sort_key_query, optional_query)
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
                        
                    json_response = sort_response(json_response, field_name, other_field, reverse_order)['Items']
                else:
                    json_response = json_response['Items']
        
                print(str(len(json_response)) + " Results")        
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

def is_valid_range_query(range_query):
    range_query = range_query.split("-")
    if len(range_query) != 2:
        return False

    try:
        start_range = int(range_query[0])
        end_range = int(range_query[1])
    except ValueError as e:
        return False
        
    return True

def execute_query(primary_key_query, sort_key_query, rating_key_query):
    dynamo_db = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamo_db.Table("Movies")
    filter_exp = ""
    response = None

    if primary_key_query:
        primary_key_query = primary_key_query.split("-")

        if len(primary_key_query) == 2:
            start_range = int(primary_key_query[0])
            end_range = int(primary_key_query[1])     
            filter_exp = Key('year').between(start_range, end_range)   
        elif len(primary_key_query) == 1:
            primary_key_value = int(primary_key_query[0])
            filter_exp = Key('year').eq(primary_key_value)
    
    if sort_key_query:
        sort_key_query = sort_key_query.split("-")

        if len(sort_key_query) == 2:
            start_range = sort_key_query[0]
            end_range = sort_key_query[1]
            if filter_exp:
                filter_exp = filter_exp & Key('title').between(start_range, end_range)
            else:
                filter_exp = Key('title').between(start_range, end_range)
        elif len(sort_key_query) == 1:
            if filter_exp:
                filter_exp = filter_exp & Key('title').eq(sort_key_query[0])
            else:
                filter_exp = Key('title').eq(sort_key_query[0])

    if rating_key_query:
        rating_key_filter = {}
        rating_filter_value = decimal.Decimal(rating_key_query['value'])

        if rating_key_query['type'] == 'gt':
            rating_key_filter = Key('info.rating').gt(rating_filter_value)
        elif rating_key_query['type'] == 'lt':
            rating_key_filter = Key('info.rating').lt(rating_filter_value)

        if filter_exp:
            filter_exp = filter_exp & rating_key_filter
        else:
            filter_exp = rating_key_filter

    response = table.scan(
        FilterExpression=filter_exp
    )
    return response

def sort_response(json, field_name, other_field, reverse_order):
    if other_field:
        json['Items'].sort(key=lambda item: item['info'][field_name], reverse=reverse_order)
    else:
        json['Items'].sort(key=lambda item: item[field_name], reverse=reverse_order)
    return json

def build_table(json_response, column_names):
    output_table = PrettyTable()
    output_table.field_names = column_names

    for item in json_response:
        row_values = []
        for i in range(len(column_names)):
            splitted = column_names[i].split(".")    
            if splitted[0] == "info":
                if splitted[1] not in item["info"].keys():
                    item["info"][splitted[1]] = "N/A"
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
    dynamo_db_client = boto3.client('dynamodb', 'us-east-1')
    main_loop(dynamo_db_client)