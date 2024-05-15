import psycopg2
import csv
from dotenv import dotenv_values

# Load database credentials from .env file
env_vars = dotenv_values(".env")
DB_NAME = env_vars["DB_NAME"]
DB_USER = env_vars["DB_USER"]
DB_PASSWORD = env_vars["DB_PASSWORD"]
DB_HOST = env_vars["DB_HOST"]
DB_PORT = 5432
# Define the SQL query to be executed
SQL_QUERY = """
SELECT * FROM your_table;
"""

# Define the filename for the CSV file
CSV_FILENAME = "output.csv"

try:
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

    # Create a cursor object to execute SQL queries
    cursor = connection.cursor()

    # Execute the SQL query
    cursor.execute(SQL_QUERY)

    # Fetch all the rows returned by the query
    rows = cursor.fetchall()

    # Open the CSV file in write mode with UTF-8 encoding
    with open(CSV_FILENAME, "w", newline="", encoding="utf-8") as csvfile:
        # Create a CSV writer object
        csv_writer = csv.writer(csvfile)

        # Write the column headers
        csv_writer.writerow([desc[0] for desc in cursor.description])

        # Write each row of the result set to the CSV file
        csv_writer.writerows(rows)

    # Close the cursor and the connection
    cursor.close()
    connection.close()

    print("success.")

except (Exception, psycopg2.Error) as error:
    print("error conection:", error)