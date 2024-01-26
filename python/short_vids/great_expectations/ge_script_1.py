import great_expectations as ge 
import pandas as pd

# Data representing products in our online electronics store
data = {
    'product': ['phone case', 'phone charger', 'tablet cover', 'laptop sleeve'],
    'category': ['Electronics', 'Accessories', 'Accessories', 'Laptops'],  
    'price': [15, 20, 12, 18],
    'in_stock': [True, True, False, True]
}

# Create a dataframe from our dataset
df = pd.DataFrame(data)

# Convert the Pandas dataframe to a GX one
ge_df = ge.dataset.PandasDataset(df)


# DQ Test 1: The values in the 'category' column should be in the predefined list
ge_df.expect_column_values_to_be_in_set('category', ['Electronics', 'Accessories', 'Fashion', 'Toys'])


# DQ Test 2: The row count should be 4 & column count should be 5
ge_df.expect_table_row_count_to_equal(4)
ge_df.expect_table_column_count_to_equal(5)

# Validate the data against the set expectations
results = ge_df.validate()
print(results)