from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import logging

def return_snowflake_conn():
    """
    This function returns a Snowflake connection cursor.
    It initializes the SnowflakeHook and retrieves a cursor for querying.
    """
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(database, schema, table, select_sql, primary_key=None):
    """
    This task creates a temp table using CTAS, checks for primary key and duplicate rows,
    creates the main table, and swaps it with the temp table.
    """
    logging.info(f"Running CTAS for table: {table}")
    logging.info(f"SQL for table creation: {select_sql}")

    cur = return_snowflake_conn()

    try:
        # Use the correct warehouse
        cur.execute("USE WAREHOUSE COMPUTE_WH;")
        
        # Create the temp table using the provided SELECT SQL statement
        create_temp_table_sql = f"CREATE TABLE IF NOT EXISTS {database}.{schema}.temp_{table} AS {select_sql}"
        logging.info(f"Executing: {create_temp_table_sql}")
        cur.execute(create_temp_table_sql)

        # Primary key uniqueness check
        if primary_key is not None:
            primary_key_check_sql = f"""
            SELECT {primary_key}, COUNT(1) AS cnt 
            FROM {database}.{schema}.temp_{table}
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 1"""
            logging.info(f"Executing primary key check: {primary_key_check_sql}")
            cur.execute(primary_key_check_sql)
            result = cur.fetchone()

            # Handle case where no result is returned
            if result is None:
                logging.info(f"No duplicate primary keys found in table temp_{table}.")
            else:
                logging.info(f"Primary key check result: {result}")
                if int(result[1]) > 1:
                    raise Exception(f"Primary key uniqueness failed: {result}")

        # Duplicate records check
        total_count_sql = f"SELECT COUNT(1) FROM {database}.{schema}.temp_{table};"
        logging.info(f"Executing: {total_count_sql}")
        cur.execute(total_count_sql)
        total_count = cur.fetchone()[0]
        logging.info(f"Total row count in temp_{table}: {total_count}")

        distinct_count_sql = f"SELECT COUNT(1) FROM (SELECT DISTINCT * FROM {database}.{schema}.temp_{table});"
        logging.info(f"Executing: {distinct_count_sql}")
        cur.execute(distinct_count_sql)
        distinct_count = cur.fetchone()[0]
        logging.info(f"Distinct row count in temp_{table}: {distinct_count}")

        # Compare total row count and distinct row count to check for duplicates
        if total_count != distinct_count:
            logging.error(f"Duplicate rows found in table temp_{table}. Total rows: {total_count}, Distinct rows: {distinct_count}")
            raise Exception(f"Duplicate records found in table temp_{table}")
        else:
            logging.info(f"No duplicates found in table temp_{table}")

        # Create the main table if it doesn't exist
        main_table_creation_sql = f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} AS
        SELECT * FROM {database}.{schema}.temp_{table} WHERE 1=0;
        """
        logging.info(f"Executing: {main_table_creation_sql}")
        cur.execute(main_table_creation_sql)

        # Swap the main table with the temp table
        swap_sql = f"""ALTER TABLE {database}.{schema}.{table} SWAP WITH {database}.{schema}.temp_{table};"""
        logging.info(f"Executing: {swap_sql}")
        cur.execute(swap_sql)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        raise

        
with DAG(
    dag_id='ELT_Summary',
    start_date=datetime(2024, 10, 3),
    catchup=False,
    tags=['ELT'],
    schedule_interval='45 3 * * *'
) as dag:
    """
    Airflow DAG for executing the CTAS operation, primary key check, duplicate record check,
    and swapping the temp table with the main table.
    """

    # Parameters for the CTAS operation
    database = "dev"
    schema = "analytics"
    table = "session_summary"
    select_sql = """SELECT u.*, s.ts
                    FROM dev.raw.user_session_channel u
                    JOIN dev.raw.session_timestamp s ON u.sessionId=s.sessionId
                """
    # Execute the task
    run_ctas(database, schema, table, select_sql, primary_key='sessionId')
