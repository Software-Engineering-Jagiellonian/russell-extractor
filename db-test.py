import psycopg2

try:
    connection = psycopg2.connect(user="postgres",
                                  password="admin",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="frege")
    # cursor = connection.cursor()
    # Executing a SQL query to insert data into  table
    # insert_query = """INSERT INTO repositories (repo_id, git_url) values (4, 'fake-url')"""
    # cursor.execute(insert_query)
    # connection.commit()
    # print("1 record inserted successfully")
    # Execute a select query
    select_query = "SELECT repo_id, git_url from repositories"
    # select_query = "SELECT 1+2"
    connection.cursor().execute(select_query)
    rs = connection.cursor().fetchall()
    print("Rs:", rs)

except (Exception, psycopg2.Error) as error:
    print("Error in transaction:", error)
finally:
    if (connection):
        connection.cursor().close()
        connection.close()
        print("Postgres connection closed")