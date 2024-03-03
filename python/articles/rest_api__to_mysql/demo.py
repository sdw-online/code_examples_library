import os
import requests
import pandas as pd
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Load API key to make API requests
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')


# Set up API request headers to authenticate requests
headers = {
    'X-RapidAPI-Key': RAPIDAPI_KEY,
    'X-RapidAPI-Host': 'api-football-v1.p.rapidapi.com'
}

# Set up API URL and parameters
url = "https://api-football-v1.p.rapidapi.com/v3/players/topscorers"
params = {"league":"39","season":"2023"}  


def check_rate_limits():
    """
    Check the API quota allocated to your account
    """
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    daily_limits = response.headers.get('x-ratelimit-requests-limit')
    daily_remaining = response.headers.get('x-ratelimit-requests-remaining')
    calls_per_min_allowed = response.headers.get('X-RateLimit-Limit')
    calls_per_min_remaining = response.headers.get('X-RateLimit-Remaining')
    
    rate_limits = {
        'daily_limit': daily_limits,
        'daily_remaining': daily_remaining,
        'minute_limit': calls_per_min_allowed,
        'minute_remaining': calls_per_min_remaining
    }

    print(rate_limits)




def get_top_scorers(url, headers, params):
    """
    Fetch the top scorers using the API 
    
    """
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.HTTPError as http_error_message:
        print (f"❌ [HTTP ERROR]: {http_error_message}")
    
    except requests.exceptions.ConnectionError as connection_error_message:
        print (f"❌ [CONNECTION ERROR]: {connection_error_message}")
    
    except requests.exceptions.Timeout as timeout_error_message:
        print (f"❌ [TIMEOUT ERROR]: {timeout_error_message}")
    
    except requests.exceptions.RequestException as other_error_message:
        print (f"❌ [UNKNOWN ERROR]: {other_error_message}")

def process_top_scorers(data):
    """
    Parse the JSON data required for the top scorers 
    """
    top_scorers = []
    for scorer_data in data['response']:
        statistics = scorer_data['statistics'][0]

        # Set up constants for processing data 
        player = scorer_data['player']
        player_name = player['name']
        club_name = statistics['team']['name']
        total_goals = int(statistics['goals']['total'])
        penalty_goals = int(statistics['penalty']['scored'])
        assists = int(statistics['goals']['assists']) if statistics['goals']['assists'] else 0
        matches_played = int(statistics['games']['appearences'])
        minutes_played = int(statistics['games']['minutes'])
        dob = datetime.strptime(player['birth']['date'], '%Y-%m-%d')
        age = (datetime.now() - dob).days // 365

        # Append data 
        top_scorers.append({
            'player': player_name,
            'club': club_name,
            'total_goals': total_goals,
            'penalty_goals': penalty_goals,
            'assists': assists,
            'matches': matches_played,
            'mins': minutes_played,
            'age': age
        })
    return top_scorers

# Function to convert the list of dictionaries into a pandas DataFrame
def create_dataframe(top_scorers):

    """
    Convert list of dictionaries into a Pandas dataframe and process it
    """

    df = pd.DataFrame(top_scorers)
    
    # Sort dataframe first by 'total_goals' in descending order, then by 'assists' in descending order
    df.sort_values(by=['total_goals', 'assists'], ascending=[False, False], inplace=True)
    
    # Reset index after sorting to reflect new order
    df.reset_index(drop=True, inplace=True)
    
    # Recalculate ranks based on the sorted order
    df['position'] = df['total_goals'].rank(method='dense', ascending=False).astype(int)
    
    # Specify the columns to include in the final dataframe in the desired order
    df = df[['position', 'player', 'club', 'total_goals', 'penalty_goals', 'assists', 'matches', 'mins', 'age']]
    
    return df



# Environment variables for MySQL
HOST = os.getenv('HOST')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_USERNAME = os.getenv('MYSQL_USERNAME')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')

# Database connection
def create_db_connection(host_name, user_name, user_password, db_name):
    """
    Establish a connection to the MySQL database
    """
    db_connection = None
    try:
        db_connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful ✅")

    except Error as e:
        print(f"❌ [DATABASE CONNECTION ERROR]: '{e}'")

    return db_connection

# Create table if not exists
def create_table(db_connection):
    """
    Create a table if it does not exist in the MySQL database
    
    """
    
    CREATE_TABLE_SQL_QUERY = """
    CREATE TABLE IF NOT EXISTS top_scorers (
        `position` INT,
        `player` VARCHAR(255),
        `club` VARCHAR(255),
        `total_goals` INT,
        `penalty_goals` INT,
        `assists` INT,
        `matches` INT,
        `mins` INT,
        `age` INT,
        PRIMARY KEY (`player`, `club`)
    );
    """
    try:
        cursor = db_connection.cursor()
        cursor.execute(CREATE_TABLE_SQL_QUERY)
        db_connection.commit()
        print("Table created successfully ✅")

    except Error as e:
        print(f"❌ [CREATING TABLE ERROR]: '{e}'")


# Insert data into the table
def insert_into_table(db_connection, df):
    """
    Insert or update the top scorers data in the database from the dataframe
    """
    cursor = db_connection.cursor()

    INSERT_DATA_SQL_QUERY = """
    INSERT INTO top_scorers (`position`, `player`, `club`, `total_goals`, `penalty_goals`, `assists`, `matches`, `mins`, `age`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `total_goals` = VALUES(`total_goals`),
        `penalty_goals` = VALUES(`penalty_goals`),
        `assists` = VALUES(`assists`),
        `matches` = VALUES(`matches`),
        `mins` = VALUES(`mins`),
        `age` = VALUES(`age`)
    """
    # Create a list of tuples from the dataframe values
    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]

    # Execute the query
    cursor.executemany(INSERT_DATA_SQL_QUERY, data_values_as_tuples)
    db_connection.commit()
    print("Data inserted or updated successfully ✅")



def run_data_pipeline():
    """
    Execute the ETL pipeline 
    """
    check_rate_limits()


    data = get_top_scorers(url, headers, params)

    if data and 'response' in data and data['response']:
        top_scorers = process_top_scorers(data)
        df = create_dataframe(top_scorers)
        print(df.to_string(index=False)) 

    else:
        print("No data available or an error occurred ❌")

    db_connection = create_db_connection(HOST, MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_DATABASE)
    

    # If connection is successful, proceed with creating table and inserting data
    if db_connection is not None:
        create_table(db_connection)  
        df = create_dataframe(top_scorers) 
        insert_into_table(db_connection, df)  


if __name__ == "__main__":
    run_data_pipeline()
