import psycopg2
from .config.config import config
from datetime import datetime


def connect():
    params = config("credentials.ini")
    engine = psycopg2.connect(**params)
    return engine


class DbManager:

    @staticmethod
    def _run_query(query, params):
        """Generic query execution method for all queries that do NOT
        return a result set (insert, update, delete)"""
        connection = connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query, tuple(params))
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in transaction:", error)  # Reverting all other operations of the transaction. ", error)
            connection.rollback()
            raise Exception("DbManager Error:", error)
        else:
            connection.commit()
            print("Transaction completed successfully")
        finally:
            if connection is not None:
                print("Closing connection to database")
                connection.close()

    @staticmethod
    def _run_select_query(query, params):
        """Generic query execution method for all queries that
        return a result set (select)"""
        connection = connect()
        try:
            cursor = connection.cursor()
            print("Calling database")
            cursor.execute(query, tuple(params))
            rs = cursor.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while executing query:", error)  # Reverting all other operations of the transaction. ", error)
            raise Exception("DbManager Error:", error)
        else:
            print("Query executed successfully")
            return rs
        finally:
            if connection is not None:
                print("Closing connection to database")
                connection.close()

    @staticmethod
    def select_languages():
        return DbManager._run_select_query("SELECT * FROM languages", [])

    @staticmethod
    def select_repository_languages(repo_id):
        return DbManager._run_select_query(
            "SELECT * FROM repository_language WHERE repository_id = %s",
            [repo_id]
        )

    @staticmethod
    def _insert_repository_languages(repo_id):
        # WRONG !! (_run_select_query doesn't call commit operation)
        return DbManager._run_select_query(
            "INSERT INTO repository_language (repository_id, language_id, present, analyzed) "
            "SELECT %s, languages.id, 'False', 'False' FROM languages RETURNING language_id, id",
            [repo_id]
        )

    @staticmethod
    def insert_repository_languages(repo_id):
        """Inserts repository_language entries for given repo id and ALL languages.
        :return result set of entries: (id, language_id) - id of the inserted entry,
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
            return rs
        finally:
            if connection is not None:
                print("Closing connection to database")
                connection.close()

    @staticmethod
    def update_repository_language_present(repo_id, lang_id, present=True):
        """Sets 'present' property of a repository_language entry, default True"""
        DbManager._run_query(
            "UPDATE repository_language SET present = %s "
            "WHERE repository_id = %s AND language_id = %s",
            [present, repo_id, lang_id])

    @staticmethod
    def insert_repository_language(repo_id, lang_id, present, analyzed=False):
        """Inserts repository_language entry for given repo id and language id.
        :return id of the inserted entry"""
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
        DbManager._run_query(
            "INSERT INTO repository_language_file (repository_language_id, file_path) "
            "VALUES (%s, %s)",
            [repo_lang_id, file_path]
        )

    @staticmethod
    def save_repository(entry):
        DbManager._run_query(
            "INSERT INTO repositories (repo_id, git_url, repo_url, crawl_time) "
            "VALUES (%s, %s, %s, %s)",
            [entry['git_url'], entry['repo_url'],
             datetime.now().astimezone().isoformat()]
        )
