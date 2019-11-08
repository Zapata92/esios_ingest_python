from urllib.error import HTTPError
import datetime
import html
import json
import pandas as pd
import re
import urllib

from esios_hook import EsiosHook
from postgres_hook import PostgresEsiosHook


class EsiosOperator():
    """
    Get table about Indicators Operators from esios Api
    :param token: personal authentication to use esios api
    :type token: str
    :param base_url: url to connect to esios api
    :type: str
    :table: table to load data in postgres database
    """

    def __init__(self,
                 table,
                 token,
                 base_url,
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table
        self.token = token
        self.base_url = base_url

    def _get_table_description(self, folder):
        """
        Get a JSON series
        :param indicator: series indicator
        :param start: Start date
        :param end: End date
        :return:
        """

        with open("{folder}/{table}.json".format(folder=folder,
                                                 table=self.table)) as file:
            table_info = json.loads(file.read().encode("utf8"))
        return table_info

    def create_description_df(self):
        esios = EsiosHook(self.token, self.base_url)
        result = esios.check_and_run()
        df = pd.DataFrame(data=result["indicators"], columns=[
            "id", "name", "description"])
        df["description"] = df["description"].apply(
            lambda x: re.sub("<[(/p)(/b)pb]>", "", html.unescape(x))
            .replace("</p>", "").replace("</b>", ""))
        print("Indicators Description Data has been created correctly")
        return df

    def create_indicators_df(self,
                             ind_description,
                             start_date_esios,
                             end_date_esios):
        esios = EsiosHook(self.token, self.base_url)
        counter = 0
        info_col = []
        for c in ind_description["indicadores"]:
            if counter == 0:
                json1 = esios.check_and_run(
                    c["esios_id"], start_date_esios, end_date_esios)
                if not len(json1["indicator"]["values"]) == 0:
                    df = pd.DataFrame.from_dict(json1["indicator"]["values"],
                                                orient="columns")[
                        ["datetime", "geo_id", "geo_name", "value"]]
                    df["datetime"] = df["datetime"].apply(
                        lambda x: datetime.datetime.strptime(x[:19],
                                                             "%Y-%m-%dT%H:%M:%S"))
                    df = df.rename(columns={"value": c["postgres_name"]})
                else:
                    counter -= 1
            else:
                json2 = esios.check_and_run(
                    c["esios_id"], start_date_esios, end_date_esios)
                if not len(json2["indicator"]["values"]) == 0:
                    df2 = pd.DataFrame.from_dict(json2["indicator"]["values"],
                                                 orient="columns")[
                        ["datetime", "geo_id", "geo_name", "value"]]
                    df2 = df2.rename(columns={"value": c["postgres_name"]})
                    df2["datetime"] = df2["datetime"].apply(
                        lambda x: datetime.datetime.strptime(x[:19],
                                                             "%Y-%m-%dT%H:%M:%S"))
                    df = pd.merge(
                        df, df2,
                        on=["datetime", "geo_id", "geo_name"],
                        how="outer")
                else:
                    pass
            info_col.append(c["postgres_name"])
            counter += 1
        try:
            return df, info_col
        except UnboundLocalError:
            return None

    def missing_control_df(self, df, info_col):
        df.replace(to_replace=[None], value=0, inplace=True)
        df_columns = list(df.columns)
        for c in info_col:
            if c not in df_columns:
                print("There is not data if field {}, se completa con ceros"
                      .format(c))
                df[i] = 0
            else:
                pass
        return df
        print("There are not more columns without data")

    def calculate_columns_df(self, df, sum_cols, new_col):
        try:
            df[new_col] = df[sum_cols].sum(axis=1)
        except KeyError:
            print("Column or columns in the list to sum is not in the df")
            raise
        except Exception:
            print("UnControlled Error")
            raise
        return df

    def drop_columns_df(self, df, drop_cols):
        try:
            df = df.drop(columns=[drop_cols])
        except KeyError:
            print("Some columns in the list to remove is not in df")
            raise
        except Exception:
            print("UnControlled Error")
            raise
        return df

    def groupby_time_esios_df(self, df, time_field, pk_fields, freq="60Min"):
        group = [pd.Grouper(freq=freq)] + pk_fields
        df = df.set_index(time_field).groupby(group).mean()
        df = df.reset_index()
        return df


class PostgresOperator():
    """
    Get table about Indicators Operators from esios Api
    :param token: personal authentication to use esios api
    :type token: str
    :param base_url: url to connect to esios api
    :type: str
    :table: table to load data in postgres database
    """

    def __init__(self,
                 table,
                 login,
                 password,
                 conn_type,
                 host="localhost",
                 schema="public",
                 port=None,
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table
        self.login = login
        self.password = password
        self.conn_type = conn_type
        self.host = host
        self.schema = schema
        self.port = port

    def get_max_timestamp(self):
        postgres = PostgresEsiosHook(
            self.login,
            self.password,
            self.conn_type,
            self.host,
            self.schema,
            self.port)
        query = ('select * from "{}" order by datetime desc limit 1'
                 .format(self.table))
        result = postgres.fetchone(query)
        if not result:
            result = "2018-01-01T00:00:00"
        else:
            result = postgres.fetchone(query)[0]
            result = datetime.timedelta(minutes=1) + result
            result = result.strftime("%Y-%m-%dT%H:%M:%S")
        return result
