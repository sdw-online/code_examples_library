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
     start_date=datetime(2024, 9, 28), 
    #  start_date=datetime.now() - timedelta(minutes=1), 
     tags=['message_display'])

def message_display_dag():    
    
    
    @task()
    def display_init_message():
        print("--- Let's initiate this workflow...✅---") 

    
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

    
    @task()
    def display_last_message():
        print("---  SUCCESS: Ending this workflow🏁🚨 ---")

    # Define task dependencies to ensure the messages are displayed in order
    starting_message = display_init_message()
    first_message = display_first_message()
    second_message = display_second_message()
    third_message = display_third_message()
    ending_message = display_last_message()

    starting_message >> first_message >> second_message >> third_message >> ending_message # Set task execution order

message_display_pipeline = message_display_dag()
