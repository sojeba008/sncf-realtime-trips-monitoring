from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from jours_feries_france import JoursFeries
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'sncf-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 26), 
    'retries': 1,
    'retry_delay': timedelta(minutes=0.1)
}

with DAG(
    'update_public_holidays_in_d_date',
    default_args=default_args,
    schedule_interval='0 0 1 * *',
    catchup=False,
) as dag:

    def update_public_holidays_in_d_date():
        year = datetime.now().year
        res = JoursFeries.for_year(datetime.now().year)
        sql = "CREATE TABLE IF NOT EXISTS ods.public_holidays(year INT4, name TEXT, date DATE);TRUNCATE TABLE ods.public_holidays;"
        for public_holiday in list(res.keys()):
            print(public_holiday)
            print(res[public_holiday])
            sql+=f"INSERT INTO ods.public_holidays(year, name, date) VALUES ({year}, '{public_holiday}', '{res[public_holiday]}'::DATE);"
            #res[public_holiday] = res[public_holiday].strftime("%Y%m%d")
        sql+="UPDATE dwh.d_date d SET is_public_holiday = TRUE FROM ods.public_holidays h WHERE d.date = h.date;"
        return sql

 

    update_public_holidays = PostgresOperator(
        task_id='update_public_holidays_task',
        postgres_conn_id='postgres_default',
        sql=update_public_holidays_in_d_date()
    )

    update_public_holidays
