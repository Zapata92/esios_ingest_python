
import pandas as pd
import datetime
import sys, os
import numpy as np

sys.path.append(os.getcwd())
sys.path.append("../module/")

from operators import EsiosOperator
from operators import PostgresEsiosOperator
from postgres_hook import PostgresEsiosHook

tables = ["indicadores", 
          "generacion_medida", 
          "precios", "demanda", 
          "generacion_tiemporeal"]

token = "b13af0538afc84e3723dd8f1a03ed0ba65fe0260c283ed0c85ab019b6e22d1b7"
base_url = "https://api.esios.ree.es/indicators"

host = "localhost"  
port = "5432"
database = "esios"
user = "postgres"
password = "postgres"
conn_type = "postgres"

folder = "../tables"
time_field = "datetime"
pk_fields = ["geo_id", "geo_name"]

end_date = (datetime.datetime.now() + 
            datetime.timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S")

postgres_hook = PostgresEsiosHook(user,
                                  password,
                                  conn_type,
                                  host,
                                  database,
                                  port)
for table in tables:
    esios_op = EsiosOperator(table, token, base_url)
    if table=="indicadores":
        #df = esios_op.create_description_df()
        #postgres_hook.load_df_esios(df, table, "replace")
        pass
    else:
        postgres_op = PostgresEsiosOperator(table,
                                            user,
                                            password,
                                            conn_type,
                                            host,
                                            database,
                                            port)
        start_date = postgres_op.get_max_timestamp()
        info_table = esios_op._get_table_description(folder)
        load_period = ((datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S") - 
            datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")).days)
        if load_period > 60:
            intv =  int(np.ceil(load_period/60))
            ranges = esios_op.date_range(start_date, 
                                         end_date,
                                         intv)
            for start,end in zip(ranges[:-1],ranges[1:]):
                end = (datetime.datetime.strptime(end,"%Y-%m-%dT%H:%M:%S") - 
            datetime.timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")
                df = esios_op.create_indicators_df(info_table, 
                                           start, 
                                           end)
                NoneType = type(None)
                if not isinstance(df, NoneType):
                    df = esios_op.missing_control_df(df[0],df[1])
                    if table == "precios":
                        df = df[df["geo_id"] == 3]
                    else:
                        df = esios_op.groupby_time_esios_df(df, time_field, pk_fields)
                        if table == "generacion_tiemporeal":
                            greal_renovables = ["gtreal_termica", 
                                                "gtreal_fotovoltaica",
                                                "gtreal_solartermica", 
                                                "gtreal_eolica",
                                                "gtreal_hidraulica"]
                            greal_no_renovables = ["gtreal_cogeneracion", 
                                                   "gtreal_ccombinado",
                                                   "gtreal_nuclear", 
                                                   "gtreal_carbon"]
                            greal_tconexiones = ["gtreal_intercambios", 
                                                 "gtreal_enlacebalear"]
                            greal_total = ["gtreal_renovables", 
                                           "gtreal_no_renovables",
                                           "gtreal_tconexiones"]
                            df = esios_op.calculate_columns_df(df, 
                                                               greal_renovables, 
                                                               "gtreal_renovables")
                            df = esios_op.calculate_columns_df(df,
                                                               greal_no_renovables,
                                                               "gtreal_no_renovables")
                            df = esios_op.calculate_columns_df(df,
                                                               greal_tconexiones,
                                                               "gtreal_tconexiones")
                            df = esios_op.calculate_columns_df(df,
                                                               greal_total,
                                                               "gtreal_total")
                        elif table == "generacion_medida":
                            gmedia_termica = ["gmedida_oceano_geotermica",
                                       "gmedida_biomasa"]
                            gmedida_renovables = ['gmedida_termica',
                                         'gmedida_fotovoltaica',
                                         'gmedida_solartermica',
                                         'gmedida_eolica',
                                         'gmedida_hidraulica']
                            gmedida_no_renovables = ['gmedida_cogeneracion',
                                            'gmedida_ccombinado',
                                            'gmedida_nuclear',
                                            'gmedida_carbon']
                            gmedida_internacionales = ['gmedida_eportugal',
                                                       'gmedida_iportugal',
                                                       'gmedida_efrancia',
                                                       'gmedida_ifrancia',
                                                       'gmedida_imarruecos',
                                                       'gmedida_emarruecos']
                            gmedida_total = ['gmedida_renovables',
                                             'gmedida_no_renovables',
                                             'gmedida_internacionales',
                                             'gmedida_enlacebalear']
                            df = esios_op.calculate_columns_df(df, 
                                                               gmedia_termica, 
                                                               "gmedida_termica")
                            df = esios_op.calculate_columns_df(df, 
                                                               gmedida_renovables, 
                                                               "gmedida_renovables")
                            df = esios_op.calculate_columns_df(df,
                                                               gmedida_no_renovables,
                                                               "gmedida_no_renovables")
                            df = esios_op.calculate_columns_df(df,
                                                               gmedida_internacionales,
                                                               "gmedida_internacionales")
                            df = esios_op.calculate_columns_df(df,
                                                               gmedida_total,
                                                               "gmedida_total")
                            df = esios_op.drop_columns_df(df, ["gmedida_oceano_geotermica", 
                                                               "gmedida_biomasa"]) 
                        else:
                            pass
                    postgres_hook.load_df_esios(df, table)
                    print("Load data from {} to {} in {}".format(start,
                                                          end,
                                                          table))
        else:
            NoneType = type(None)
            if not isinstance(df, NoneType):
                print("a")
                df = esios_op.create_indicators_df(info_table, 
                                           start_date, 
                                           end_date)                
                df = esios_op.missing_control_df(df[0],df[1])
                if table == "precios":
                    df = df[df["geo_id"] == 3]
                else:
                    df = esios_op.groupby_time_esios_df(df, time_field, pk_fields)
                    if table == "generacion_tiemporeal":
                        greal_renovables = ["gtreal_termica", 
                                            "gtreal_fotovoltaica",
                                            "gtreal_solartermica", 
                                            "gtreal_eolica",
                                            "gtreal_hidraulica"]
                        greal_no_renovables = ["gtreal_cogeneracion", 
                                               "gtreal_ccombinado",
                                               "gtreal_nuclear", 
                                               "gtreal_carbon"]
                        greal_tconexiones = ["gtreal_intercambios", 
                                             "gtreal_enlacebalear"]
                        greal_total = ["gtreal_renovables", 
                                       "gtreal_no_renovables",
                                       "gtreal_tconexiones"]
                        df = esios_op.calculate_columns_df(df, 
                                                           greal_renovables, 
                                                           "gtreal_renovables")
                        df = esios_op.calculate_columns_df(df,
                                                           greal_no_renovables,
                                                           "gtreal_no_renovables")
                        df = esios_op.calculate_columns_df(df,
                                                           greal_tconexiones,
                                                           "gtreal_tconexiones")
                        df = esios_op.calculate_columns_df(df,
                                                           greal_total,
                                                           "gtreal_total")
                    elif table == "generacion_medida":
                        gmedia_termica = ["gmedida_oceano_geotermica",
                                   "gmedida_biomasa"]
                        gmedida_renovables = ['gmedida_termica',
                                     'gmedida_fotovoltaica',
                                     'gmedida_solartermica',
                                     'gmedida_eolica',
                                     'gmedida_hidraulica']
                        gmedida_no_renovables = ['gmedida_cogeneracion',
                                        'gmedida_ccombinado',
                                        'gmedida_nuclear',
                                        'gmedida_carbon']
                        gmedida_internacionales = ['gmedida_eportugal',
                                                   'gmedida_iportugal',
                                                   'gmedida_efrancia',
                                                   'gmedida_ifrancia',
                                                   'gmedida_imarruecos',
                                                   'gmedida_emarruecos']
                        gmedida_total = ['gmedida_renovables',
                                         'gmedida_no_renovables',
                                         'gmedida_internacionales',
                                         'gmedida_enlacebalear']
                        df = esios_op.calculate_columns_df(df, 
                                                           gmedia_termica, 
                                                           "gmedida_termica")
                        df = esios_op.calculate_columns_df(df, 
                                                           gmedida_renovables, 
                                                           "gmedida_renovables")
                        df = esios_op.calculate_columns_df(df,
                                                           gmedida_no_renovables,
                                                           "gmedida_no_renovables")
                        df = esios_op.calculate_columns_df(df,
                                                           gmedida_internacionales,
                                                           "gmedida_internacionales")
                        df = esios_op.calculate_columns_df(df,
                                                           gmedida_total,
                                                           "gmedida_total")
                        df = esios_op.drop_columns_df(df, ["gmedida_oceano_geotermica", 
                                                           "gmedida_biomasa"]) 
                    else:
                        pass           
                    postgres_hook.load_df_esios(df, table)    