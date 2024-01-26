"""
Script Name: test_data_quality.py
Description: This script performs data quality tests on car datasets, including checks for completeness, accuracy, duplication, and quantity validation for car models.

Author: Stephen David-Williams
Creation Date: 2024-01-22
Last Updated: [N/A]


You can find the short video demos here: 
YouTube - https://youtube.com/shorts/iAWoamvb7QU?si=eu0RJMUKrNxNHuh4
TikTok -  https://www.tiktok.com/@sdw.online/video/7326641648157412641?is_from_webapp=1&sender_device=pc&web_id=7213404854906652165 



How to run:
--------------
1. Ensure the file name begins with "test_".
2. To execute the tests, type any of the following in the terminal:
   - pytest file_name.py
   - pytest -m run

Notes:
------

⏳ I'll be working on better quality videos to break down how each component of the script works - 

✅ Subscribe and follow my pages to be notified when the videos drop! 


Changelog:
----------
[Date]: [N/A]
[Date]: [N/A]
...
"""

# test_data_quality.py 

import pytest 
import pandas as pd 

@pytest.fixture
def cars_data():
    data = {
        "order_id": [101, 102, 103, 104],
        "car_model": ['Aston Martin', 'Ferrari', 'Lamborghini', 'Rolls Royce'],
        'quantity': [2, 3, 1, 4]
    }
    cars_df = pd.DataFrame(data)
    return cars_df


# Completeness test 
def test_missing_values(cars_data):
    actual_missing_values_count = cars_data.isnull().sum().sum()
    assert actual_missing_values_count == 0 


# Accuracy test 
def test_data_accuracy(cars_data):
    trusted_data = {
        'order_id': [101, 102, 103, 104],
        'car_model': ['Aston Martin', 'Ferrari', 'Lamborghini', 'Rolls Royce'], 
        'quantity': [2, 3, 1, 4]
    }
    trusted_df = pd.DataFrame(trusted_data)
    pd.testing.assert_frame_equal(cars_data, trusted_df)


# Deduplication test
def test_duplicates(cars_data):
    actual_duplicates_count = cars_data.duplicated().sum()
    assert actual_duplicates_count == 0


# Parametrized test for car model quantities
@pytest.mark.parametrize("car_model, expected_quantity", [
    ('Aston Martin', 2),
    ('Ferrari', 3),
    ('Lamborghini', 1),
    ('Rolls Royce', 4)
])
def test_car_model_quantity(cars_data, car_model, expected_quantity):
    actual_quantity = cars_data[cars_data['car_model'] == car_model]['quantity'].sum()
    assert actual_quantity == expected_quantity



