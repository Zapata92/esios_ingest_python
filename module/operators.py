from urllib.error import HTTPError
import datetime
import html
import json
import pandas as pd
import re
import numpy as np
from esios_hook import EsiosHook
from postgres_hook import PostgresEsiosHook
import os

class Operator():
    """
    Load variables json
    input
        :param vars_folder: folder path of variables file
        :type vars_folder: str
    output:
        :param tables: postgres tables name
        :type tables: list
        :param esios_hk: data about esios hook
        :type esios_hk: dict         
        :param ptgs_hook: data about postgres hook
        :type ptgs_hook: dict 
        :param script_vars: data about execute script
        :type script_vars: dict
    """

    def __init__(self,
                 vars_folder,
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vars_folder = vars_folder
    print(os.getcwd())
    def load_variables(self):
        try:
            with open("{}/variables.json".format(self.vars_folder)) as f:
                data = json.load(f)
            tables = data["tables"]
            esios_hk = data["esios_hk"]
            ptgs_hook = data["ptgs_hook"]
            script_vars = data["script_vars"]
        except FileNotFoundError:
            print("File Not Found, Check path of variables folder")
            raise
        except Exception:
            print("UnControlled Error")
            raise
        return tables, esios_hk, ptgs_hook, script_vars

        
class EsiosOperator():
    """
    Get and transformation Esios Api Data
    :param table: table name of postgres database
    :type table: str
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

    def get_table_description(self, folder):
        """
        Get table information to transform name from differents tables
        input
            :param folder: table description path folder
            :type folder: str
        output
            :param table_info: information of selected table
            :type table_info: json
        """

        with open("{folder}/{table}.json".format(folder=folder,
                                                 table=self.table)) as file:
            table_info = json.loads(file.read().encode("utf8"))
        return table_info

    def create_description_df(self):
        """
        Create description dataframe with description of diferents Dataframes
        output
            :param df: idescription indicators dataframe
            :type df: df
        """        
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
        """
        Create dataframe from differents indicators data depending of the table
        input
            :param ind_description: description info from _get_table_description
            :type ind_description: json
            :start_date_esios: start date from wich data is downloaded
            :type str: format %Y-%m-%dT%H:%M:%S
            :end_date_esios: end date until wich data is downloaded
            :type str: format %Y-%m-%dT%H:%M:%S
        output
            :param df: indicators df
            :type df: df
        """    
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
        """
        Check if existing missing values and replace by 0
        input
            :param df: indicators df
            :type df: Pandas Dataframe
            :param info_col: list of indicators to ingest
            :type info_col: list
        output
            :param df: indicators df
            :type df: df
        """    
        df.replace(to_replace=[None], value=0, inplace=True)
        df_columns = list(df.columns)
        for c in info_col:
            if c not in df_columns:
                print("There is not data if field {}, se completa con ceros"
                      .format(c))
                df[c] = 0
            else:
                pass
        return df
        print("There are not more columns without data")

    def date_range(self, start, end):
        """
        Create list of time intervals
        input
            :start: start date from wich data is downloaded
            :type str: format %Y-%m-%dT%H:%M:%S
            :end: end date until wich data is downloaded
            :type str: format %Y-%m-%dT%H:%M:%S
        output
            :param ranges: list of time intervals
            :type ranges: list
        """  
        ranges = []
        start_dt = datetime.datetime.strptime(start,"%Y-%m-%dT%H:%M:%S")
        end_dt = datetime.datetime.strptime(end,"%Y-%m-%dT%H:%M:%S")
        load_period = (end_dt - start_dt).days
        if load_period > 60:
            intv =  int(np.ceil(load_period/60))
            diff = (end_dt  - start_dt ) / intv
            for i in range(intv):
                ranges.append((start_dt + diff * i).strftime("%Y-%m-%dT%H:%M:%S"))
            ranges.append(end_dt.strftime("%Y-%m-%dT%H:%M:%S"))
            ranges_intv = []
            for tm in ranges:
                tm = datetime.datetime.strptime(tm,"%Y-%m-%dT%H:%M:%S")
                tm = tm - datetime.timedelta(minutes=tm.minute % 10,
                                 seconds=tm.second)
                ranges_intv.append(tm.strftime("%Y-%m-%dT%H:%M:%S"))
            return ranges_intv    
        else:
            ranges = [start, end]
            return ranges

    def calculate_columns_df(self, df, sum_cols, new_col):
        """
        Create calculate columns, is the sum of other field
        input
            :param df: indicators df
            :type df: Pandas Dataframe
            :param sum_cols: list of indicators to sum
            :type sum_cols: list
            :param new_col: name of new column
            :type new_col: string
        output
            :param df: indicators df
            :type df: df
        """ 
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
        """
        Drop specifics columns
        input
            :param df: indicators df
            :type df: Pandas Dataframe
            :param drop_cols: list of indicators to drop
            :type drop_cols: list
        output
            :param df: indicators df
            :type df: df
        """ 
        try:
            df = df.drop(columns=[drop_cols])
        except KeyError:
            print("Some columns in the list to remove is not in df")
            pass
        except Exception:
            print("UnControlled Error")
            raise
        return df

    def groupby_time_esios_df(self, df, time_field, pk_fields, freq="60Min"):
        """
        Group data by hours by time and geolocalization
        input
            :param df: indicators df
            :type df: Pandas Dataframe
            :param time_field: name of time field
            :type time_field: str
            :param pk_fields: list of primary key fields
            :type pk_fields: str
        output
            :param df: indicators df
            :type df: Pandas Dataframe
        """ 
        group = [pd.Grouper(freq=freq)] + pk_fields
        df = df.set_index(time_field).groupby(group).mean()
        df = df.reset_index()
        return df


class PostgresEsiosOperator():
    """
    Interact with Postgres Hook to connect with postgres Database.
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
    :param table: table name to execute query
    :type table: str
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

    def get_max_timestamp(self, table, publicacion):
        """
        Get latest timestamp load, and return specif date if table not exist 
        """      
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
            result = "2016-01-01T00:00:00"
        else:
            result = postgres.fetchone(query)[0]
            result = datetime.timedelta(minutes=publicacion) + result
            result = result.strftime("%Y-%m-%dT%H:%M:%S")
        return result
