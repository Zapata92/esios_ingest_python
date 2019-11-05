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

    :param postgres_conn_id: connection id to connect with postgres_hook
    :type postgres_conn_id: str
    :param method: the API method to be called
    :type method: str
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
        conn = self.get_conn()
        login = ""
        if conn.login:
            login = ("{login}:{pass}@".format(login=self.login,
                                              pass=self.pass))
        host = self.host
        if self.port is not None:
            host += ":{port}".format(port=self.port)

        uri = ("{conn_type}://{login}{host}/{schema}"
               .format(conn_type=self.conn_type,
                       login=self.login,
                       host=self.host,
                       schema=self.schema))
        return uri

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        if engine_kwargs is None:
            engine_kwargs = {}
        uri = self.get_uri()
        engine = create_engine(uri, **engine_kwargs)
        return engine

    def get_connection(self, engine_kwargs=None):
        engine = self.get_sqlalchemy_engine(engine_kwargs)
        try:
            connection = engine.raw_connection()
        except exc.OperatioalError:
            print("OperationalError: "
                  "Unable to connect to Server of Postgres database")
            raise
        except Exception:
            print("UnControlled Error")
            raise
        return connection

    def check_table(self, table, engine_kwargs=None):
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
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
        except psycopg2.ProgrammingError as e:
            print("ProgrammingError:\n", e)
            raise
        except Exception:
            print("UnControlled Error")
            raise
        finally:
            connection.close()
        return result

    def fetchall(self, query):
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
        except psycopg2.ProgrammingError as e:
            print("ProgrammingError:\n", e)
            raise
        except Exception:
            print("UnControlled Error")
            raise
        finally:
            connection.close()
        return results

    def fetch_df(self, query, engine_kwargs=None):
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
