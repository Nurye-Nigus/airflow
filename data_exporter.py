from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd
from sqlalchemy import create_engine
import urllib
import os

# Define my database connection details
DB_USER = "postgres"
DB_PASSWORD = "Nurye@68793"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "airflow"

# Define the path to my CSV file
DAG_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(DAG_DIRECTORY, 'dataset', 'cardataset.csv')

# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG
dag = DAG(
    'nurye_dag',
    default_args=default_args,
    description='nurye you are uploading this car dataset',
    schedule_interval='@daily',  # Adjust the schedule interval as needed
)

# Task to read CSV file and save vehicle_data.csv and trajectory_data.csv locally
def process_csv_file():
    try:
        # Read the CSV file
        with open(CSV_FILE_PATH, 'r') as file:
            lines = file.readlines()

        print(f"Lines read from CSV file: {lines}")

        # Process the lines to create vehicle_data.csv and trajectory_data.csv locally
        lines_as_lists = [line.strip('\n').strip().strip(';').split(';') for line in lines]
        cols = lines_as_lists.pop(0)
        vehicle_col = cols[:4]
        trajectory_col = ['track_id'] + cols[4:]

        track_info = []
        trajectory_info = []

        for row in lines_as_lists:
            track_id = row[0]

            # add the first 4 values to track_info080
            
            track_info.append(row[:4])

            remaining_values = row[4:]
            # reshape the list into a matrix and add track_id
            trajectory_matrix = [[track_id] + remaining_values[i:i + 6] for i in range(0, len(remaining_values), 6)]
            # add the matrix rows to trajectory_info
            trajectory_info.extend(trajectory_matrix)

        # Convert to dataframes
        vehicle_data = pd.DataFrame(track_info, columns=vehicle_col)
        trajectory_data = pd.DataFrame(trajectory_info, columns=trajectory_col)

        # Save CSV files locally
        vehicle_data.to_csv(os.path.join(DAG_DIRECTORY, 'dataset', 'vehicle_data.csv'), index=False)
        trajectory_data.to_csv(os.path.join(DAG_DIRECTORY, 'dataset', 'trajectory_data.csv'), index=False)

        print("CSV files created successfully.")
    except Exception as e:
        print(f"Error processing CSV file: {str(e)}")
        raise

# Task to create tables in the Airflow database
def create_tables():
    # Create tables in the Airflow database
    create_vehicle_table = """
    CREATE TABLE IF NOT EXISTS vehicle_data (
        track_id VARCHAR(255),
        type VARCHAR(255), 
        traveled_D VARCHAR(255), 
        avg_speed VARCHAR(255)
    )
    """
    create_trajectory_table = """
    CREATE TABLE IF NOT EXISTS trajectory_data (
        lat VARCHAR(255),
        lon VARCHAR(255),
        speed VARCHAR(255),
        lon_acc VARCHAR(255),
        lat_acc VARCHAR(255),
        time VARCHAR(255)
    )
    """
    # Execute SQL queries to create tables
    for query in [create_vehicle_table, create_trajectory_table]:
        PostgresOperator(
            task_id='create_table',
            sql=query,
            postgres_conn_id='car_con',  # Update with your connection ID
            dag=dag,
        ).execute(context=None)

# Task to load data into PostgreSQL
def load_data_into_postgres():
    # Database connection details
    engine = create_engine(f"postgresql://{DB_USER}:{urllib.parse.quote(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    # Load DataFrames into the PostgreSQL database
    vehicle_data = pd.read_csv(os.path.join(DAG_DIRECTORY, 'dataset', 'vehicle_data.csv'))
    trajectory_data = pd.read_csv(os.path.join(DAG_DIRECTORY, 'dataset', 'trajectory_data.csv'))

    vehicle_data.to_sql('vehicle_data', engine, if_exists='replace', index=False)
    trajectory_data.to_sql('trajectory_data', engine, if_exists='replace', index=False)

with dag:
    # Task to process the CSV file and save locally
    process_csv_task = PythonOperator(
        task_id='process_csv',
        python_callable=process_csv_file,
    )

    # Task to create tables in the Airflow database
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables,
    )

    # Task to load data into PostgreSQL
    load_data_task = PythonOperator(
        task_id='load_data_into_postgres',
        python_callable=load_data_into_postgres,
    )

    # Set task dependencies
    process_csv_task >> create_tables_task >> load_data_task
