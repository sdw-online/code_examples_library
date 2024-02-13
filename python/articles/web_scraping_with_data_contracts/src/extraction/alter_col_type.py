from utils.db_utils import connect_to_db


def alter_column_to_varchar(connection, schema_name, table_name, column_name):
    cursor = connection.cursor()
    alter_statement = f"""
    ALTER TABLE {schema_name}.{table_name}
    ALTER COLUMN "{column_name}" TYPE VARCHAR;
    """
    
    try:
        cursor.execute(alter_statement)
        print(f"✅ Column {column_name} type changed to VARCHAR.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()

if __name__ == "__main__":
    # Connect to the database
    connection = connect_to_db()
    
    # Alter the 'Team' column to 'VARCHAR'
    alter_column_to_varchar(connection, 'raw', 'scraped_fb_data', 'team')
    
    # Close the database connection
    connection.close()
