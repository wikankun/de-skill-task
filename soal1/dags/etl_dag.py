import time
import pandas as pd
from datetime import datetime, timedelta
import sqlite3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


TIMESTAMP = time.strftime("%Y%m%d")
OUTPUT_DB = './data/sqlite.db'


default_args = {
    'owner': 'wikan',
    "depends_on_past": False,
}


with DAG(
    'etl_dag',
    schedule_interval='0 0 * * *',
    default_args=default_args,
    description='ETL eFishery Task 1',
    start_date=days_ago(1),
    tags=['efishery'],
) as dag:
    dag.doc_md = __doc__

    def extract_transform(**kwargs):
        ti = kwargs['ti']
        output_target = './data/fact_order_accumulating.csv'

        # read sql query and load it to sqlite
        conn = sqlite3.connect(OUTPUT_DB)
        cur = conn.cursor()

        tables = []
        query = "SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';"
        for row in cur.execute(query):
            tables.append(row[0])

        print(tables)
        # read db and write to dataframe
        with conn:
            df = {}
            for table in tables:
                df[table] = pd.read_sql_query("SELECT * FROM {}".format(table), conn)
                df[table].columns = df[table].columns.str.lower()

        # transform it
        df['payments'] = df['payments'].merge(df['invoices'], how='left', on='invoice_number')
        df['orders'] = df['orders'].merge(df['customers'], how='left', left_on='customer_id', right_on='id')
        df['invoices'] = df['invoices'].merge(df['orders'], how='left', on='order_number')
        df['order_lines'] = df['order_lines'].merge(df['invoices'], how='left', on='order_number')
        df['order_lines'] = df['order_lines'].merge(df['products'], how='left', left_on='product_id', right_on='id')

        output_df = df['order_lines'].drop(columns=['id_x', 'id_y'])
        output_df = output_df.rename(columns={'name_x': 'customer_name', 'name_y': 'item_name'})

        # save dataframe to csv file
        output_df.to_csv(output_target, index=False, header=True, quoting=2)


    def load(**kwargs):
        ti = kwargs['ti']
        input_target = './data/fact_order_accumulating.csv'
        conn = sqlite3.connect(OUTPUT_DB)
            
        # read csv file
        with conn:
            df = pd.read_csv(input_target)
            # load it to sqlite
            df.to_sql('fact_order_accumulating', conn, if_exists='replace', index=False)


    start = DummyOperator(
        task_id='start',
    )
    
    extract_transform_task = PythonOperator(
        task_id='extract_transform',
        python_callable=extract_transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> extract_transform_task >> load_task >> end
