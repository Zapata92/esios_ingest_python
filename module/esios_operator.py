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
                 token,
                 base_url,
                 table,
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = token
        self.base_url = base_url
        self.table = table

    def load_description_table(self):
        postgres = PostgresHook()
        esios = EsiosHook(self.token, self.base_url)
        result = esios.check_and_run()
        df = pd.DataFrame(data=result["indicators"], columns=[
            'id', 'name', 'description'])
        df['description'] = df['description'].apply(
            lambda x: re.sub("<[(/p)(/b)pb]>", "", html.unescape(x))
            .replace('</p>', '').replace("</b>", ""))
        return df
        postgres.load_df_esios(df, self.table, if_exists="replace")
        print("Indicators Description Data has been loaded correctly")


class LatestTimestampOperator(BaseOperator):
    """
    Get data latest timestamp from postgres table and use to the next operator
    :param table: table to fetch latest timestamp
    :type table: str
    """
    template_fields = ()
    ui_color = '#f0eee4'

    def __init__(self,
                 table,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table

    def execute(self, context):
        postgres = PostgresEsiosHook()
        query = ('select * from "{}" order by datetime desc limit 1'
                 .format(self.table))
        result = postgres.fetchone(query)
        if not result:
            result = "2014-01-01T00:00:00"
        else:
            result = postgres.fetchone(query)[0]
            result = datetime.timedelta(minutes=1) + result
            result = result.strftime("%Y-%m-%dT%H:%M:%S")
        self.log.info("latest timestamp: {}".format(result))
        context['ti'].xcom_push(key='max_date', value=result)


class EsiosOperator(BaseOperator):
    """
    Get data from esios Api, save and transform to the target result
    and send to postgres table
    :param token: personal authentication to use esios api
    :type token: str
    :param base_url: url to connect to esios api
    :type: str
    """
    template_fields = ('start_date_esios',)
    ui_color = '#f0eee4'

    def __init__(self,
                 token,
                 base_url,
                 table,
                 start_date_esios=None,
                 end_date_esios=None,
                 * args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = token
        self.base_url = base_url
        self.table = table
        self.start_date_esios = start_date_esios
        self.end_date_esios = end_date_esios

    def _get_table_description_(self):
        """
        Get a JSON series
        :param indicator: series indicator
        :param start: Start date
        :param end: End date
        :return:
        """

        with open('/airflow/tables/{}.json'.format(self.table)) as file:
            table_info = json.loads(file.read().encode('utf8'))
        return table_info

    def _etl_sendpostgres_(self, postgres, esios, ind_description,
                           start_date_esios_b, end_date_esios_b):
        counter = 0
        info_col = []        
        for c in ind_description["indicadores"]:
            #print("Parsiong {}".format(c['esios_name']))
            if counter == 0:
                json1 = esios.check_and_run(
                    c["esios_id"], start_date_esios_b, end_date_esios_b)
                if not len(json1['indicator']['values']) == 0:
                    df = pd.DataFrame.from_dict(json1["indicator"]["values"],
                                                orient='columns')[
                        ['datetime', 'geo_id', 'geo_name', 'value']]
                    df["datetime"] = df["datetime"].apply(
                        lambda x: datetime.datetime.strptime(x[:19],
                                                             "%Y-%m-%dT%H:%M:%S"))
                    df = df.rename(columns={"value": c['postgres_name']})
                else:
                    counter -= 1
            else:
                json2 = esios.check_and_run(
                    c["esios_id"], start_date_esios_b, end_date_esios_b)
                if not len(json2['indicator']['values']) == 0:
                    df2 = pd.DataFrame.from_dict(json2["indicator"]["values"],
                                                 orient='columns')[
                        ['datetime', 'geo_id', 'geo_name', 'value']]
                    df2 = df2.rename(columns={"value": c['postgres_name']})
                    df2["datetime"] = df2["datetime"].apply(
                        lambda x: datetime.datetime.strptime(x[:19],
                                                             "%Y-%m-%dT%H:%M:%S"))
                    df = pd.merge(
                        df, df2,
                        on=['datetime', 'geo_id', 'geo_name'],
                        how='outer')
                else:
                    pass
            counter += 1
        try:
            df.replace(to_replace=[None], value=0, inplace=True)
            df_columns = list(df.columns)
            for i in info_col:
                if i not in df_columns:
                    print(i)
                    df[i] = 0
                else:
                    pass
        except UnboundLocalError:
            pass
        try:
            if self.table == "generacion_tiemporeal":
                df['gtreal_renovables'] = df[['gtreal_termica',
                                              'gtreal_fotovoltaica',
                                              "gtreal_solartermica",
                                              "gtreal_eolica",
                                              "gtreal_hidraulica"]].sum(axis=1)
                df['gtreal_no_renovables'] = df[['gtreal_cogeneracion',
                                                 'gtreal_ccombinado',
                                                 'gtreal_nuclear',
                                                 'gtreal_carbon']].sum(axis=1)
                df['gtreal_tconexiones'] = df[['gtreal_intercambios',
                                               'gtreal_enlacebalear']].sum(axis=1)
                df['gtreal_total'] = df[['gtreal_renovables',
                                         'gtreal_no_renovables',
                                         'gtreal_tconexiones']].sum(axis=1)

            elif self.table == "generacion_medida":
                df["gmedida_termica"] = df[['gmedida_oceano_geotermica',
                                            'gmedida_biomasa']].sum(axis=1)
                df["gmedida_renovables"] = df[['gmedida_termica',
                                               'gmedida_fotovoltaica',
                                               'gmedida_solartermica',
                                               'gmedida_eolica',
                                               'gmedida_hidraulica']].sum(axis=1)
                df['gmedida_no_renovables'] = df[['gmedida_cogeneracion',
                                                  'gmedida_ccombinado',
                                                  'gmedida_nuclear',
                                                  'gmedida_carbon']].sum(axis=1)
                df['gmedida_internacionales'] = df[['gmedida_eportugal',
                                                    'gmedida_iportugal',
                                                    'gmedida_efrancia',
                                                    'gmedida_ifrancia',
                                                    'gmedida_imarruecos',
                                                    'gmedida_emarruecos']].sum(axis=1)
                df['gmedida_total'] = df[['gmedida_renovables',
                                          'gmedida_no_renovables',
                                          'gmedida_internacionales',
                                          'gmedida_enlacebalear']].sum(axis=1)
                df.drop(columns=['gmedida_oceano_geotermica', 'gmedida_biomasa'])
            else:
                pass
        except UnboundLocalError:
            pass
        if self.table == "precios":
            if not df.empty:
                df = df[df["geo_id"] == 3]
                postgres.load_df_esios(df, self.table)
            else:
                pass
        else:
            if not df.empty:
                df = df.set_index('datetime').groupby(
                    [pd.Grouper(freq='60Min'), 'geo_id', 'geo_name']).mean()
                df = df.reset_index()
                postgres.load_df_esios(df, self.table)
            else:
                pass

    def execute(self, context):
        esios = EsiosHook(self.token, self.base_url)
        postgres = PostgresEsiosHook()

        ind_description = self._get_table_description_()
        dt_start_date_esios = datetime.datetime.strptime(
            self.start_date_esios, "%Y-%m-%dT%H:%M:%S")
        dt_end_date_esios = datetime.datetime.strptime(
            self.end_date_esios, "%Y-%m-%dT%H:%M:%S")
        period = datetime.timedelta(days=30)

        start_date_esios_b = dt_start_date_esios
        if ((dt_end_date_esios - dt_start_date_esios).days) > 30:
            while start_date_esios_b < dt_end_date_esios:
                end_date_esios_b = start_date_esios_b+period
                if dt_end_date_esios < end_date_esios_b:
                    end_date_esios_b = dt_end_date_esios
                else:
                    pass
                start_date_esios_b_str = start_date_esios_b.strftime(
                    "%Y-%m-%dT%H:%M:%S")
                end_date_esios_b_str = end_date_esios_b.strftime(
                    "%Y-%m-%dT%H:%M:%S")
                self._etl_sendpostgres_(
                    postgres, esios, ind_description,
                    start_date_esios_b_str, end_date_esios_b_str)
                self.log.info("Ingested data of table {} from {} to {}".format(
                    self.table, start_date_esios_b_str, end_date_esios_b_str))
                start_date_esios_b = end_date_esios_b + \
                    datetime.timedelta(seconds=1)
        else:
            self._etl_sendpostgres_(
                postgres, esios, ind_description,
                self.start_date_esios, self.end_date_esios)
            self.log.info("Ingested data of table {} from {} to {}".format(
                self.table, self.start_date_esios, self.end_date_esios))


class AirflowEsiosPlugin(AirflowPlugin):
    name = "esios"
    operators = [
        EsiosOperator,
        LatestTimestampOperator,
        EsiosIndicatorsOperator]
    sensors = []
    hooks = [
        EsiosHook,
        PostgresEsiosHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
