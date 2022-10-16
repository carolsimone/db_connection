__author__ = 'simonecarolini'

import psycopg2
import psycopg2.extras
import os

import pandas as pd

password = os.environ['PG_PASSWORD']  # Hard code you password if you want to make it work.


# Re-use of connection
class Borg:
    __shared_state = {}

    def __init__(self):
        self.__dict__ = self.__shared_state


class PostgreSQLConnector(Borg):

    def __init__(self,
                 host: str,
                 database: str,
                 user: str,
                 ):
        super().__init__()
        self.host = host
        self.database = database
        self.user = user

    @property
    def conn(self):
        if not hasattr(self, '_conn'):
            self._conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=password,
                port=5432
            )
            self._conn.autocommit = True
        return self._conn

    def get_conn(self):
        return self.conn

    def read_pg(self, custom_sql: str):
        """
        Read from the Postgres instance and convert the retrieved data into a DataFrame format so that you can
        easily manipulate with pandas.

        :params
        -------
        custom_sql: str
            String containing your fully functioning SQL code as you would write in your preferred IDE.

        :return
        -------
        df: dataframe
            You get in return a pandas dataframe with the data you asked for.
        """
        try:
            cursor = self.get_conn().cursor()
            cursor.execute(custom_sql)
            cols = [desc[0] for desc in cursor.description]
            list_tuples = cursor.fetchall()
            return pd.DataFrame([row for row in list_tuples], columns=cols)
        except(psycopg2.DatabaseError, Exception) as error:
            print(f'Following error experienced while reading from Postgres:\n\t{error}')
        finally:
            cursor.close()
        return

    def read_pg_raw(self, custom_sql: str):
        """
        Read from the Postgres instance and convert the retrieved data into a list. I would always recommend to use
        the `read_pg` method because return a pandas dataframe which is highly convenient for most of the tasks.

        :params
        -------
        custom_sql: str
            String containing your fully functioning SQL code as you would write in your preferred IDE.

        :return
        -------
        ls: list
            It will return a list(tuples).
        """
        try:
            cursor = self.get_conn().cursor()
            cursor.execute(custom_sql)
            return cursor.fetchall()
        except(psycopg2.DatabaseError, Exception) as error:
            print(f'Following error experienced while reading from Postgres:\n\t{error}')
        finally:
            cursor.close()
        return

    def run_(self, custom_sql: str):
        """
        Run commands like drop, truncate, create.

        :param
        ------
        custom_sql: str
            SQL query, mostly DDL command should be run through it.

        :return
        -------
        None
        """
        try:
            cursor = self.get_conn().cursor()
            cursor.execute(custom_sql)
            self.get_conn().commit()
        except(psycopg2.DatabaseError, Exception) as error:
            print(f'Command not executed due to the following error:\n\t{error}')
        finally:
            cursor.close()
        return

    def load_pg(self, df: pd.DataFrame, schema_name: str, table_name: str) -> None:
        """
        Load data to database. This method uses `copy_from` which is considered one of the most performing methods
        based on psycopg2 documentation, a good read is the following:
        https://hakibenita.com/fast-load-data-python-postgresql.

        :params
        -------
        df: dataframe
            Dataframe you want to load

        table_name: str
            Define the table name within the schema of your choice.

        :return
        -------
        None:
            Load data to Postgres instance.
        """

        # TODO: Remove commas to avoid issue while loading text
        cursor = self.get_conn().cursor()
        df.to_csv('df.csv', index=False)
        try:
            with open('df.csv', 'r') as f:
                next(f)  # Skip the header row
                cursor.copy_from(f, f'{schema_name}.{table_name}', sep=',')
        except(psycopg2.DatabaseError, Exception) as error:
            print(f'Error while loading the data. Following error:\n\t{error}')
        finally:
            self.get_conn().commit()
            os.remove('df.csv')
