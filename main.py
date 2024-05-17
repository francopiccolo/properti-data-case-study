import urllib.request
import zipfile
import sqlite3
from pathlib import Path
import os, shutil
import great_expectations as gx

from config import CODES_MAPPING, DIM_CODES, DB_PATH

def remove_data(folder):
    """Cleans the data folder in order to load a new batch of data"""
    for filename in os.listdir(folder):
        if filename == '.gitkeep':
            continue
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

def download_file(url, file, folder): 
    """Downloads and unzips the file with the Swiss housing info"""
    print('Downloading file ', file)
    urllib.request.urlretrieve(url, file)

    print('Unzipping file ', file)
    with zipfile.ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall(folder)

def create_dim_tables():
    """Creates dimension tables""" 
    sqliteConnection = sqlite3.connect(DB_PATH)
    cursor = sqliteConnection.cursor()

    for code in DIM_CODES:
        dim = CODES_MAPPING[code]
        cursor.executescript(f'DROP TABLE IF EXISTS dim_{dim}; CREATE TABLE dim_{dim} AS SELECT CAST(cecodid AS INTEGER) AS {dim}_code, codtxtli AS {dim}_italian FROM codes WHERE cmerkm = "{code}"')

def create_fact_tables():
    """Creates fact tables""" 
    sqliteConnection = sqlite3.connect(DB_PATH)
    cursor = sqliteConnection.cursor()
    for table in ['building', 'dwelling', 'entrance']:
        print('Creating fact table', table)
        query = Path(f'./sql/fact/{table}.sql').read_text()
        cursor.executescript(query)

def run_tests():
    """Runs tests to ensure data quality"""
    sqliteConnection = sqlite3.connect(DB_PATH)
    cursor = sqliteConnection.cursor()
    for table in ['building', 'dwelling', 'entrance']:
        print('Running tests for table', table)
        cursor.execute(f'SELECT name FROM PRAGMA_TABLE_INFO("{table}");')
        data = cursor.fetchall()
        field_codes = [code[0] for code in data]
        dims = []
        for field_code in field_codes:
            if field_code in DIM_CODES:
                dims.append(CODES_MAPPING[field_code])
        run_referential_integrity_tests(table, dims)

def build_referential_integrity_query(table, dim):
    """Builds a query on which we will run a referential integrity test"""
    query = f"""
        SELECT t2.{dim}_code
        FROM fact_{table} t1
        LEFT JOIN dim_{dim} t2
        WHERE t1.{dim}_code IS NOT NULL"""
    return query

def run_referential_integrity_tests(table, dims):
    """Runs referential integrity tests on one fact table against all the dimension tables"""
    context = gx.get_context()
    my_connection_string = f'sqlite:///{DB_PATH}'
    datasource_name = 'sqlite'
    datasource = context.sources.add_sqlite(
        name=datasource_name, 
        connection_string=my_connection_string
    )
    validations = []
    for dim in dims:
        asset_name = f'{dim} query'
        query = build_referential_integrity_query(table, dim)
        datasource.add_query_asset(name=asset_name, query=query)
        batch_request = datasource.get_asset(f'{dim} query').build_batch_request()
        expectation_suite_name = f'{table} {dim} expectation suite'
        context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=expectation_suite_name,
        )

        validator.expect_column_values_to_not_be_null(column=f'{dim}_code')
        validator.save_expectation_suite(discard_failed_expectations=False)
        validations.append({
                "batch_request": batch_request,
                "expectation_suite_name": expectation_suite_name,
            })
    
    checkpoint_name = 'Checkpoint'
    checkpoint = context.add_or_update_checkpoint(
        name=checkpoint_name,
        validations=validations,
    )

    checkpoint_result = checkpoint.run()
    # context.view_validation_result(checkpoint_result) # Uncomment to view test results in UI
    if not checkpoint_result.success:
        raise Exception('Tests failed')

def pipeline():
    """Executes the whole data pipeline, can be ran on a schedule to update the data model"""
    remove_data('./data')
    download_file('https://public.madd.bfs.admin.ch/ch.zip', './data/ch.zip', './data')
    create_dim_tables()
    create_fact_tables()
    run_tests()
    
if __name__ == '__main__':
    try:
        pipeline()
    except Exception as e:
        raise('Pipeline failed')