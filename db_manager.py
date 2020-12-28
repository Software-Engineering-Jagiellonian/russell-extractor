import psycopg2
from .config.config import config
from datetime import datetime


def connect():
    params = config("credentials.ini")
    engine = psycopg2.connect(**params)
    return engine


class DbManager:
    # TODO: add generic method that takes the query as a parameter

    @staticmethod
    def select_languages():
        connection = connect()
        rs = None
        try:
            cursor = connection.cursor()
            print("Calling database")
            query = "SELECT * FROM languages"
            cursor.execute(query)
            # print(cursor.description)
            rs = cursor.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while executing query:", error)  # Reverting all other operations of the transaction. ", error)
            raise
        else:
            print("Query executed successfully")
        finally:
            if connection is not None:
                print("Closing connection to database")
                connection.close()
                return rs

    @staticmethod
    def select_repository_languages(repo_id):
        connection = connect()
        rs = None
        try:
            cursor = connection.cursor()
            query = "SELECT * FROM repository_language WHERE repository_id = %s"
            cursor.execute(query, (repo_id,))
            rs = cursor.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while executing query:", error)  # Reverting all other operations of the transaction. ", error)
            raise
        else:
            print("Query executed successfully")
        finally:
            if connection is not None:
                print("Closing connection to database")
                connection.close()
                return rs

    @staticmethod
    def insert_repository_languages(repo_id):
        """Inserts repository_language entries for given repo id and ALL languages.
        Returns result set of entries: (id, language_id) - id of the inserted entry,
        and id of its language"""
        rs = None
        connection = connect()
        try:
            cursor = connection.cursor()
            query = "INSERT INTO repository_language (repository_id, language_id, present, analyzed)" \
                    "SELECT %s, languages.id, 'False', 'False' FROM languages RETURNING language_id, id"
            cursor.execute(query, (repo_id,))
            rs = cursor.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in transaction:", error)  # Reverting all other operations of the transaction. ", error)
            connection.rollback()
            raise
        else:
            connection.commit()
            print("Transaction completed successfully")
        finally:
            if connection is not None:
                print("Closing connection to database")
                connection.close()
                return rs

    '''Sets 'present' property of a repository_language entry, default True'''
    @staticmethod
    def update_repository_language_present(repo_id, lang_id, present=True):
        connection = connect()
        try:
            cursor = connection.cursor()
            query = "UPDATE repository_language SET present = %s " \
                    "WHERE repository_id = %s AND language_id = %s"
            cursor.execute(query, (present, repo_id, lang_id))
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in transaction:", error)  # Reverting all other operations of the transaction. ", error)
            connection.rollback()
            raise
        else:
            connection.commit()
            print("Transaction completed successfully")
        finally:
            if connection is not None:
                print("Closing connection to database")
                connection.close()

    @staticmethod
    def insert_repository_language(repo_id, lang_id, present, analyzed=False):
        connection = connect()
        rs = None
        try:
            cursor = connection.cursor()
            query = "INSERT INTO repository_language (repository_id, language_id, present, analyzed)" \
                    "VALUES (%s, %s, %s, %s) RETURNING id"
            cursor.execute(query, (repo_id, lang_id, present, analyzed))
            rs = cursor.fetchone()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in transaction:", error)  # Reverting all other operations of the transaction. ", error)
            connection.rollback()
            raise
        else:
            connection.commit()
            print("Transaction completed successfully")
        finally:
            if connection is not None:
                print("Closing connection to database")
                connection.close()
                return rs

    @staticmethod
    def insert_repository_language_file(repo_lang_id, file_path):
        connection = connect()
        try:
            cursor = connection.cursor()
            query = "INSERT INTO repository_language_file (repository_language_id, file_path)" \
                    "VALUES (%s, %s)"
            cursor.execute(query, (repo_lang_id, file_path))
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in transaction:", error)  # Reverting all other operations of the transaction. ", error)
            connection.rollback()
            raise
        else:
            connection.commit()
            print("Transaction completed successfully")
        finally:
            if connection is not None:
                print("Closing connection to database")
                connection.close()

    @staticmethod
    def save_repository(entry):
        if not entry:
            return
        connection = connect()
        try:
            print("Calling database")
            query = "INSERT INTO repositories (repo_id, git_url, repo_url, crawl_time) VALUES (%s, %s, %s, %s);"
            connection.cursor().execute(query, (entry['repo_id'], entry['git_url'], entry['repo_url'], datetime.now().astimezone().isoformat()))
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in transaction Reverting all other operations of a transaction ", error)
            connection.rollback()
            raise
        else:
            connection.commit()
            print("Transaction completed successfully")
        finally:
            if connection is not None:
                print("Closing connection to database")
                connection.close()
