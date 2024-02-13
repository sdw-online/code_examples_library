import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def connect_to_db():
    HOST=os.getenv('HOST')
    PORT=os.getenv('PORT')
    DATABASE=os.getenv('DATABASE')
    POSTGRES_USERNAME=os.getenv('POSTGRES_USERNAME')
    POSTGRES_PASSWORD=os.getenv('POSTGRES_PASSWORD')
    # Use environment variables directly
    try:
        db_connection = psycopg2.connect(
            host=HOST,
            port=PORT,
            dbname=DATABASE,
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD,
        )
        db_connection.set_session(autocommit=True)
        print("✅ Connection to the database established successfully.")
        return db_connection
    except Exception as e:
        raise Exception(f"❌[ERROR - DB CONNECTION]: Error connecting to the database: {e}")

        




def create_extracted_schema_and_table(db_connection, schema_name, table_name):
    cursor = db_connection.cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    db_connection.commit()

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            "pos" INTEGER,
            "team" TEXT NOT NULL,
            "p" INTEGER,
            "w1" INTEGER,
            "d1" INTEGER,
            "l1" INTEGER,
            "gf1" INTEGER,
            "ga1" INTEGER,
            "w2" INTEGER,
            "d2" INTEGER,
            "l2" INTEGER,
            "gf2" INTEGER,
            "ga2" INTEGER,
            "gd" INTEGER,
            "pts" INTEGER,
            "date" DATE
        )
    """
    cursor.execute(create_table_query)
    db_connection.commit()
    cursor.close()

def insert_extracted_data_to_table(db_connection, schema_name, table_name, dataframe):
    cursor = db_connection.cursor()
    for index, row in dataframe.iterrows():
        data = tuple(row)
        placeholders = ",".join(["%s"] * len(row))
        insert_query = f"""
            INSERT INTO {schema_name}.{table_name} (
                "pos", "team", "p", "w1", "d1", "l1", "gf1", "ga1", "w2", "d2", "l2", "gf2", "ga2", "gd", "pts", "date"
            )
            VALUES ({placeholders})
        """
        try:
            cursor.execute(insert_query, data)
            db_connection.commit()
        except Exception as e:
            print(f"❌Failed to insert data: {data}❌. Error: {e}")
    cursor.close()





# Transformation
    
def create_transformed_schema_and_table(db_connection, schema_name, table_name):
    cursor = db_connection.cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    db_connection.commit()

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            "position" INTEGER,
            "team_name" VARCHAR,
            "games_played" INTEGER,
            "wins" INTEGER,
            "draws" INTEGER,
            "losses" INTEGER,
            "goals_for" INTEGER,
            "goals_against" INTEGER,
            "goal_difference" INTEGER,
            "points" INTEGER,
            "match_date" DATE
        )
    """
    cursor.execute(create_table_query)
    db_connection.commit()
    cursor.close()

# Function to fetch data from the extraction layer
def fetch_extraction_data(db_connection, schema_name, table_name):
    query = f"SELECT * FROM {schema_name}.{table_name};"
    return pd.read_sql(query, db_connection)


def insert_transformed_data_to_table(db_connection, schema_name, table_name, dataframe):
    cursor = db_connection.cursor()
    
    # Check the dataframe columns before insertion
    # print(f"Dataframe columns: {dataframe.columns.tolist()}")
    
    # Building column names for the INSERT INTO statement
    columns = ', '.join([f'"{c}"' for c in dataframe.columns])
    
    # Building placeholders for the VALUES part of the INSERT INTO statement
    placeholders = ', '.join(['%s' for _ in dataframe.columns])

    # Construct the INSERT INTO statement
    insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders})"

    # Execute the INSERT INTO statement for each row in the dataframe
    for index, row in dataframe.iterrows():
        # Print the row to be inserted for debugging purposes
        # print(f"Row data: {tuple(row)}")
        try:
            cursor.execute(insert_query, tuple(row))
            db_connection.commit()
        except Exception as e:
            print(f"❌Failed to insert transformed data: {tuple(row)}❌. Error: {e}")
    cursor.close()



# Loading
def fetch_transformed_data(db_connection):
    print("Fetching transformed data from the database...")
    try:
        query = "SELECT * FROM staging.transformed_fb_data;"
        df = pd.read_sql(query, db_connection)
        print("Data fetched successfully.")
        return df
    except Exception as e:
        raise Exception(f"❌[ERROR - FETCH DATA]: {e}")


def convert_dataframe_to_csv(df, filename):
    target_destination = 'data/'
    full_file_path = f"{target_destination}{filename}"
    print(f"Converting dataframe to CSV ('{filename}')...")
    try:
        df.to_csv(full_file_path, index=False)
        print(f"CSV file '{filename}' created and saved to target destination successfully.")
        print(f"Full file path: '{full_file_path}'")
    except Exception as e:
        raise Exception(f"❌[ERROR - CSV CREATION]: {e}")