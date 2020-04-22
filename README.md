# AWS/Azure Storage Applications

Python scripts to compare AWS and Azure storage products. In particular, AWS S3 with Azure Blob Storage, and DynamoDB with CosmosDB.

<br>

## Prerequisites
    
* Install all python project dependencies.
    ```
    pipenv install
    ```

* Make sure your .aws/credentials file is up to date.
* Set AZURE_STORAGE_CONNECTION_STRING environment variable with your blob storage connection string.
* Change database connection string in [config.py](./config.py) to your cosmosDB instance.

<br>

## Run

* Load/delete data from S3.
    ```
    pipenv run python3 createPopContainersAWS.py
    pipenv run python3 createPopContainersAWS.py reset //teardown
    ```

* Load/delete data from Azure Blob Storage.
    ```
    pipenv run python3 createPopContainersAzure.py
    pipenv run python3 createPopContainersAzure.py reset //teardown
    ```

* Run S3 client program.
    ```
    pipenv run python3 storageApplianceAWS.py
    ```

* Run Blob Storage client program.
    ```
    pipenv run python3 storageApplianceAzure.py
    ```

* Load movie data into DynamoDB.
    ```
    pipenv run python3 createMovieDBAWS.py
    ```

* Load movie data into CosmosDB
    ```
    pipenv run python3 createMovieDBAzure.py
    ```

* Run DynamoDB client program.
    ```
    pipenv run python3 queryDBAWS.py
    ```

* Run CosmosDB client program.
    ```
    pipenv run python3 queryDBAzure.py
    ```

<br>

## Known Limitations

* For DynamoDB client by default the projected attributes are year, title, and info.rating. User can add more but can't take these away.
