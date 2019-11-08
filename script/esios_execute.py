
import pandas as pd
import datetime
import sys, os

sys.path.append(os.getcwd())
sys.path.append("../module/")

from operators import EsiosOperator
from postgres_hook import PostgresEsiosHook

token = "b13af0538afc84e3723dd8f1a03ed0ba65fe0260c283ed0c85ab019b6e22d1b7"
base_url = "https://api.esios.ree.es/indicators"
folder = "../tables"
esios_op = EsiosOperator("generacion_medida", token, base_url)
info_table = esios_op._get_table_description(folder)
df = esios_op.create_indicators_df(info_table, "2019-01-01T10:00:00",  "2019-01-01T12:00:00")
df = esios_op.missing_control_df(df[0],df[1])