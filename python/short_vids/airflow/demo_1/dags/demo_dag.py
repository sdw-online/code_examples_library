from airflow.decorators import dag, task
from datetime import datetime, timedelta

# Add default arguments for demo
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),  # Retry a failed task after 1 minute
}

@dag(default_args=default_args, 
     schedule_interval='*/2 * * * *', 
     start_date=datetime.now() - timedelta(minutes=1), 
     tags=['message_display'])

def message_display_dag():
    
    # Add task for displaying the 1st message 
    @task()
    def display_first_message():
        print("This is the 1st message (1/3)🔴") 


    # Add task for displaying the 2nd message 
    @task()
    def display_second_message():
        print("This is the 2nd message (2/3)🟡")  


    # Add task for displaying the 3rd message 
    @task()
    def display_third_message():
        print("This is the 3rd message (3/3)🟢")  


    # Define task dependencies to ensure the messages are displayed in order
    first_message = display_first_message()
    second_message = display_second_message()
    third_message = display_third_message()

    first_message >> second_message >> third_message  # Set task execution order

message_display_pipeline = message_display_dag()