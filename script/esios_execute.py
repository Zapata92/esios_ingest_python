
import pandas as pd
import datetime
import sys, os

sys.path.append(os.getcwd())
sys.path.append("../module/")

from operators import EsiosOperator
from postgres_hook import PostgresEsiosHook

tables = ["indicadores", "generacion_medida", "precios", "demanda", "generacion_tiemporeal"]
token = "b13af0538afc84e3723dd8f1a03ed0ba65fe0260c283ed0c85ab019b6e22d1b7"
base_url = "https://api.esios.ree.es/indicators"
folder = "../tables"
time_field = "datetime"
pk_fields = ["geo_id", "geo_name"]
for table in tables:
    esios_op = EsiosOperator(table, token, base_url)
    if table=="indicadores":
        df = esios_op.create_description_df()
    else:
        info_table = esios_op._get_table_description(folder)
        df = esios_op.create_indicators_df(info_table, 
                                           "2019-01-01T10:00:00", 
                                           "2019-01-01T11:50:00")
        df = esios_op.missing_control_df(df[0],df[1])
        if table == "precios":
            df = df[df["geo_id"] == 3]
        else:
            df = esios_op.groupby_time_esios_df(df, time_field, pk_fields)