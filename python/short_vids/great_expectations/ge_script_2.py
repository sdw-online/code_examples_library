import os
import pandas as pd
import great_expectations as ge


# 1. Extract the data (E)
def extract_data(file_path):
    return pd.read_csv(file_path)


"""

I corrected the error message that appeared from the initial extract_data function with this code block:

def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)
        # Ensure 'unit_price' is of type float
        if 'unit_price' in df.columns:
            df['unit_price'] = df['unit_price'].astype(float)
        return df
    except Exception as e:
        raise Exception(f"An error occurred during data extraction: {e}")
"""


def validate_extracted_data(df):
    ge_df = ge.from_pandas(df)
    
    # The following columns should exist in the dataframe
    ge_df.expect_column_to_exist('product_id')
    ge_df.expect_column_to_exist('product_name')
    ge_df.expect_column_to_exist('unit_price')
    ge_df.expect_column_to_exist('quantity_sold')

    # Each column should follow the schema specification provided
    ge_df.expect_column_values_to_be_of_type('product_id', 'int64')
    ge_df.expect_column_values_to_be_of_type('product_name', 'object') 
    ge_df.expect_column_values_to_be_of_type('unit_price', 'float64')
    ge_df.expect_column_values_to_be_of_type('quantity_sold', 'int64')
    

    # Run the validation checks
    results = ge_df.validate()
    if not results['success']:
        raise ValueError("Validation failed for extracted data: " + str(results))
    return results


# 2. Transform the data (T)
def transform_data(df):
    try:
        if 'unit_price' not in df or 'quantity_sold' not in df:
            raise KeyError("Required columns for transformation are missing.")
        df['total_sales'] = df['unit_price'] * df['quantity_sold']
        return df
    except KeyError as e:
        raise e
    except Exception as e:
        raise Exception(f"An error occurred during transformation: {e}")



def validate_transformed_data(df):
    ge_df = ge.from_pandas(df)
    
    # The 'total_sales' column should exist
    ge_df.expect_column_to_exist('total_sales')

    # The 'total_sales' column should be a float
    ge_df.expect_column_values_to_be_of_type('total_sales', 'float')
    
		# Run the validation checks
    results = ge_df.validate()
    if not results['success']:
        raise ValueError("Validation failed for transformed data")
    return results


# 3. Load the data (L)
def load_data(df, file_path):
    df.to_csv(file_path, index=False)



def validate_loaded_data(file_path):
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        raise FileNotFoundError(f"The file {file_path} was not found or is empty.")

    print(f"File {file_path} has been successfully validated.")


# Run the data pipeline (ETL)
def run_etl_pipeline(extract_file_path, load_file_path):

    # 1. Extract (E)
    extracted_data = extract_data(extract_file_path)
    if validate_extracted_data(extracted_data):
        print("Extracted data is valid.")
    
    # 2. Transform (T)
    transformed_data = transform_data(extracted_data)
    if validate_transformed_data(transformed_data):
        print("Transformed data is valid.")
    
    # 3. Load (L)
    load_data(transformed_data, load_file_path)
    validate_loaded_data(load_file_path)
    print("Data has been successfully loaded and validated.")


run_etl_pipeline('source_sales_data.csv', 'target_sales_data.csv')