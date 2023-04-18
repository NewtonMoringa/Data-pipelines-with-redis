import pandas as pd
import psycopg2
import redis
import pickle

# Redis Cloud Instance Information
redis_host = 'redis-15995.c299.asia-northeast1-1.gce.cloud.redislabs.com'
redis_port = '15995'
redis_password =  'Luce9K03Bq0ngfS9oHVyhPzrlP2CjcAu'

# Postgres Database Information
pg_host = 'localhost'
pg_database = 'pipeline'
pg_user = 'newton'
pg_password = '1234'

# Redis Client Object
redis_client = redis.Redis(host=redis_host, port=redis_port, password=redis_password)


def extract_data():
    # Extract data from CSV file using pandas
    data = pd.read_csv('customer_call_logs.csv')
    
    
    # Cache data in Redis for faster retrieval
    redis_client.set('customer_call_logs', pickle.dumps(data))
    
def transform_data():
    # Retrieve data from Redis cache
    transformed_data = pickle.loads(redis_client.get('customer_call_logs'))

    # Transform data (clean, structure, format)
    # Convert call_date to datetime
    transformed_data['call_date'] = pd.to_datetime(transformed_data['call_date'])
    # Drop null values
    transformed_data = transformed_data.dropna()

    return transformed_data

def load_data():
    # Connect to Postgres database
    conn = psycopg2.connect(host=pg_host, database=pg_database, user=pg_user, password=pg_password)

    # Create a cursor object
    cur = conn.cursor()

    # Create a table to store the data
    cur.execute('CREATE TABLE IF NOT EXISTS customer_call_logs (\
                 customer_id INT,\
                 call_cost_usd FLOAT,\
                 call_destination VARCHAR,\
                 call_date TIMESTAMP,\
                 call_duration_min FLOAT\
                 )')

    # Insert the transformed data into the database
    for i, row in transformed_data.iterrows():
        cur.execute(f"INSERT INTO customer_call_logs (customer_id, call_cost_usd, call_destination, call_date, call_duration_min) VALUES ({row['customer_id']}, {row['call_cost_usd']}, '{row['call_destination']}', '{row['call_date']}', {row['call_duration_min']})")

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

def data_pipeline():
    # Data pipeline function
    extract_data()
    transformed_data = transform_data()
    load_data(transformed_data)

if __name__ == '__main__':
    # Run the data pipeline function
    data_pipeline()