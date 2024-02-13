from utils.db_utils import connect_to_db

def add_column(connection, schema_name, table_name, column_name, data_type):
    cursor = connection.cursor()
    alter_statement = f"""
    ALTER TABLE {schema_name}.{table_name}
    ADD COLUMN IF NOT EXISTS "{column_name}" {data_type};
    """
    
    try:
        cursor.execute(alter_statement)
        connection.commit()
        print(f"✅ Column '{column_name}' added to table '{schema_name}.{table_name}'.")
    except Exception as e:
        print(f"❌ An error occurred when adding '{column_name}': {e}")
    finally:
        cursor.close()

def add_new_columns(connection, schema_name, table_name, columns_info):
    for column_name, data_type in columns_info.items():
        add_column(connection, schema_name, table_name, column_name, data_type)

if __name__ == "__main__":
    # Connect to the database
    connection = connect_to_db()
    
    # Schema and table names
    schema_name = 'staging'
    table_name = 'transformed_fb_data'
    
    # New columns to add with their respective data types
    new_columns_info = {
        'wins': 'INTEGER',
        'draws': 'INTEGER',
        'losses': 'INTEGER'
    }
    
    # Add new columns to the table
    add_new_columns(connection, schema_name, table_name, new_columns_info)
    
    # Close the database connection
    connection.close()
