# --------------------------------
# LIBRARIES
# --------------------------------

# Import Airflow Operators --done

from cgitb import text
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
# Import Libraries to handle dataframes
import pandas as pd
import numpy as np
import os

# Import Library to send Slack Messages
import requests
# Import Library to set DAG timing
from datetime import timedelta, datetime

# --------------------------------
# CONSTANTS
# --------------------------------

# Set input and output paths incomplete 

FILE_PATH_INPUT = '/home/airflow/gcs/data/input/'
FILE_PATH_OUTPUT = '/home/airflow/gcs/data/output/'

# Import CSV into a pandas dataframe

file_name =os.listdir(FILE_PATH_INPUT)[0]
df = pd.read_csv(FILE_PATH_INPUT + file_name)


# Slack webhook link -- done 
slack_webhook =  'https://hooks.slack.com/services/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'

# --------------------------------
# FUNCTION DEFINITIONS
# --------------------------------

# Function to send messages over slack using the slack_webhook -- done 

def send_msg(text_string): requests.post(slack_webhook, json={'text': text_string})
 

  # Function to generate the diagnostics report
# Add print statements to each variable so that it appears on the Logs
def send_report():

#1. Average O2 level
    average_o2_level = df['o2_level'].mean()
    print('Avg Oxygen level is {}'. format(average_o2_level))

#2. Average Heart Rate

    average_heart_rate = df['heart_rate'].mean()
    print('Avg Heart rate is {}'. format(average_heart_rate))

#3. Standard Deviation of O2 level
    std_Oxygen_level = df['o2_level'].std()
    print('Standard Deviation of O2 level is {}'. format(std_Oxygen_level))

#4. Standard Deviation of Heart Rate
    std_heart_rate = df['heart_rate'].std()
    print('Standard Deviation of Heart Rate is {}'. format(std_heart_rate))

#5. Minimum O2 level
    min_Oxg_level = df['o2_level'].min()
    print('Minimum Oxygen level is {}'. format(min_Oxg_level))

#6. Minimum Heart Rate
    min_heart_rate = df['heart_rate'].min()
    print('Minimum Heart Rate is {}'. format(min_heart_rate))

#7. Maximum O2 level
    max_Oxg_level = df['o2_level'].max()
    print('Maximum Oxygen level is {}'.format(max_Oxg_level))

#8. Maximum Heart Rate
    max_heart_rate = df['heart_rate'].max()
    print('Maximum Heart Rate is {}'. format(max_heart_rate))

    text_string = '''
 Average O2 level {0}
 Average Heart Rate {1}
 Standard Deviation of O2 level {2}
 Standard Deviation of Heart {3}
 Minimum O2 level {4}
 Minimum Heart Rate {5}
 Minimum Heart Rate {6}
 Maximum O2 level {7}
 '''.format(average_o2_level,average_heart_rate,
           std_Oxygen_level,std_heart_rate,
           min_Oxg_level,min_heart_rate,
           max_Oxg_level,max_heart_rate)

    send_msg(text_string)


#3 Function to filter anomalies in the data
# Add print statements to each output dataframe so that it appears on the Logs
def flag_anomaly():
    """
    As per the patient's past medical history, below are the mu and sigma 
    for normal levels of heart rate and o2:
    
    Heart Rate = (80.81, 10.28)
    O2 Level = (96.19, 1.69)

    Only consider the data points which exceed the (mu + 3*sigma) or (mu - 3*sigma) levels as anomalies
    Filter and save the anomalies as 2 dataframes in the output folder - hr_anomaly_P0015.csv & o2_anomaly_P0015.csv
    """
heart_mu =80.81
heart_sigma = 10.28

heart_upper = heart_mu+ 3* heart_sigma
heart_lower = heart_mu- 3* heart_sigma

heart_anamolies_df = df[(df['heart_rate'] > heart_upper) | (df['heart_rate'] < heart_lower)]
heart_anamolies_df.to_csv(f"{FILE_PATH_OUTPUT}/hr_anomaly_P0015.csv", index=False)
    
oxygen_mu  = 96.19
oxygen_sigma  = 1.69

oxygen_upper = oxygen_mu + 3*oxygen_sigma
oxygen_lower = oxygen_mu - 3*oxygen_sigma

oxygen_anamolies_df = df[(df['heart_rate'] > oxygen_upper) | (df['o2_level'] < oxygen_lower)]
oxygen_anamolies_df.to_csv(f"{FILE_PATH_OUTPUT}/o2_anomaly_P0015.csv", index=False)



                                                     
# --------------------------------
# DAG definition
# --------------------------------

# Define the defualt args

default_args = {
    'owner': 'user_name',
    'start_date': datetime(2022, 11, 14),
    'depends_on_past': False,
    'email': ['mohammad.rousan@outlook.com'], # Add your burner account email address
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# Create the DAG object

with DAG(
    'health_ops',
    default_args=default_args,
    description='DAG to monitor heartrate and O2 levels',
    schedule_interval='*/15 * * * *',
    catchup=False
) as dag:
    # start_task
    start_task = DummyOperator(
        task_id='start',
        dag=dag
    ) 
    # file_sensor_task
    file_sensor = FileSensor(
        task_id='file_sensor',
        poke_interval=15,
        filepath=FILE_PATH_INPUT,
        timeout=5,
        dag=dag
    )
    # send_report_task
    send_slack_report = PythonOperator(
        task_id='send_slack_report',
        python_callable=send_report,
        dag=dag,
        op_kwargs={'df':df}
    )
    # flag_anomaly_task
    flag_anomaly_task=PythonOperator(
        task_id='flag_anomaly_task',
        python_callable=flag_anomaly,
        dag=dag,
        op_kwargs={'df':df}
    )
    # end_task
    end_task = DummyOperator(
        task_id='end',
        dag=dag
    )
    
   
    
    
   
# Set the dependencies

# --------------------------------
start_task >> file_sensor >> [send_slack_report, flag_anomaly_task] >> end_task