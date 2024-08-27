import psycopg2
import pandas as pd


def insert_data_to_redshift(selected_city_details_list):
    """
    Inserts the given list of city details into a Redshift table.

    Args:
        selected_city_details_list: A list of dictionaries containing city details.
    """

    # Replace with your Redshift connection details
    conn_string = "host='your_host' dbname='your_database' user='your_user' password='your_password' port='5439'"

    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        # Assuming a table named 'city_details' with columns matching your data
        table_name = 'city_details'

        # Convert list of dictionaries to a Pandas DataFrame for efficient insertion
        df = pd.DataFrame(selected_city_details_list)

        # Create an SQL statement for inserting data
        columns = ', '.join(df.columns)
        values = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

        # Execute the insert query for each row in the DataFrame
        for index, row in df.iterrows():
            cur.execute(insert_query, tuple(row))

        conn.commit()
        cur.close()
        conn.close()

        print("Data inserted successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
