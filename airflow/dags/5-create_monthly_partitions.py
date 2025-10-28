from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'sncf-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 26), 
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'create_monthly_partitions',
    default_args=default_args,
    schedule_interval='0 0 1 * *',
    catchup=False,
) as dag:

    def generate_partition_sql():
        today = date.today()
        end_date = today + relativedelta(months=1)

        ods_tables = ["ods.trips", "ods.stops"]
        dwh_tables = ["dwh.f_trips", "dwh.f_journey"]

        current_date = today
        sql_statements = []

        while current_date < end_date:
            next_date = current_date + timedelta(days=1)

            suffix = current_date.strftime("%Y%m%d")
            tk_current = int(current_date.strftime("%Y%m%d"))
            tk_next = int(next_date.strftime("%Y%m%d"))

            for table in ods_tables:
                sql_statements.append(f"""
CREATE TABLE IF NOT EXISTS {table}_{suffix} PARTITION OF {table}
    FOR VALUES FROM ('{current_date}') TO ('{next_date}');
""")

            for table in dwh_tables:
                sql_statements.append(f"""
CREATE TABLE IF NOT EXISTS {table}_{suffix} PARTITION OF {table}
    FOR VALUES FROM ({tk_current}) TO ({tk_next});
""")
            current_date = next_date

        return "\n".join(sql_statements)

    create_partitions = PostgresOperator(
        task_id='create_monthly_partitions_task',
        postgres_conn_id='postgres_default',
        sql=generate_partition_sql()
    )

    create_partitions
