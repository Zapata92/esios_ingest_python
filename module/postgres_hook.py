# -*- coding: utf-8 -*-
"""
@author: Jorge Zapata Muñoz
"""
from sqlalchemy import create_engine
from sqlalchemy import exc
import pandas as pd
import psycopg2


class PostgresEsiosHook():
    """
    Interact with Postgres Server.

    :param login: user of postgres database
    :type login: str
    :param password: password of postgres database
    :type password: str
    :param conn_type: database type
    :type conn_type: str
    :param host: host database server
    :type host: str
    :param schema: database name
    :type schema: str
    :param port: port database server
    :type port: str
    """

    def __init__(
        self,
        login,
        password,
        conn_type,
        host="localhost",
        schema="public",
        port=None
    ):
        self.login = login
        self.password = password
        self.conn_type = conn_type
        self.host = host
        self.schema = schema
        self.port = port

    def get_uri(self):
        """
        Get uri to connect with database server
        output:
            :param uri: uri te create database connector
            :type: str
        """
        login = ""
        if self.login:
            login = ("{login}:{passw}@".format(login=self.login,
                                                    passw=self.password))
        host = self.host
        if self.port is not None:
            host += ":{port}".format(port=self.port)

        uri = ("{conn_type}://{login}{host}/{schema}"
               .format(conn_type=self.conn_type,
                       login=login,
                       host=self.host,
                       schema=self.schema))
        return uri

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        """
        Create engine from specific uri
        output:
            :param engine: engine of sqlalqchemy library
            :type: sqlalchemy.Object
        """
        if engine_kwargs is None:
            engine_kwargs = {}
        uri = self.get_uri()
        engine = create_engine(uri, **engine_kwargs)
        return engine

    def get_connection(self, engine_kwargs=None):
        """
        Create connection from engine
        output:
            :param connection: connection of sqlalqchemy library
            :type: sqlalchemy.Object
        """
        engine = self.get_sqlalchemy_engine(engine_kwargs)
        try:
            connection = engine.raw_connection()
        #except exc.OperatioalError:
        except Exception:
            print("OperationalError: "
                  "Unable to connect to Server of Postgres database")
            raise
        except Exception:
            print("UnControlled Error")
            raise
        return connection

    def check_table(self, table, engine_kwargs=None):
        """
        Check if table exist
        input:
            :param table: database table to check
            :type str
        output:
            :param check: true or false depending if exist or not
            :type bool
        """
        try:
            engine = self.get_sqlalchemy_engine(engine_kwargs)
            check = engine.has_table(table)
        except exc.OperatioalError:
            print("OperationalError: "
                  "Unable to connect to Server of Postgres database")
            raise
        except Exception:
            print("UnControlled Error")
            raise
        return check

    def fetchone(self, query):
        """
        Execute query and return one row
        input:
            :param quey: sql query to execute
            :type str
        output:
            :param result: 
            :type tuple
        """
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
        except psycopg2.ProgrammingError as e:
            print("ProgrammingError:\n", e)
            result = None
            pass
        except Exception:
            print("UnControlled Error")
            raise
        finally:
            connection.close()
        return result

    def fetchall(self, query):
        """
        Execute query and return result
        input:
            :param quey: sql query to execute
            :type str
        output:
            :param result: 
            :type tuple
        """
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
        except psycopg2.ProgrammingError as e:
            print("ProgrammingError:\n", e)
            results = None
            pass
        except Exception:
            print("UnControlled Error")
            raise
        finally:
            connection.close()
        return results

    def fetch_df(self, query, engine_kwargs=None):
        """
        Execute query and return result
        input:
            :param quey: sql query to execute
            :type str
        output:
            :param result: 
            :type df
        """
        engine = self.get_sqlalchemy_engine(engine_kwargs)
        try:
            df = pd.read_sql_query(query, engine)
        except exc.ProgrammingError as e:
            print("ProgrammingError:\n", e)
            raise
        except Exception:
            print("UnControlled Error")
            raise
        return df

    def load_df_esios(self, df, table, if_exists="append", engine_kwargs=None):
        """
        Load dataframe to postgres database
        input:
            :param df: dataframe to load
            :type df
            :param table: target table to load df
            :type str
            :param if_exists: condition if table exist (append or replace)
            :type str
        """

        engine = self.get_sqlalchemy_engine(engine_kwargs)
        is_table = self.check_table(table, engine_kwargs)
        if is_table:
            try:
                df.to_sql(
                    table, engine, if_exists=if_exists, index=False)
            except exc.ProgrammingError as e:
                print("ProgrammingError:\n", e)
                raise
            except Exception:
                print("UnControlled Error")
                raise
        else:
            try:
                df.to_sql(table, engine, index=False)
            except exc.ProgrammingError as e:
                print("ProgrammingError:\n", e)
                raise
            except Exception:
                print("UnControlled Error")
                raise
