
import pandas as pd
import datetime
import sys
import os
sys.path.append(os.getcwd())
sys.path.append("../module/")
from postgres_hook import PostgresEsiosHook
from operators import Operator
from operators import PostgresEsiosOperator
from operators import EsiosOperator

operator = Operator("../variables")
tables, esios_hk, ptgs_hook, varbs = operator.load_variables()

# Create end timestamp, to avoid duplicated data, always must be xx:50:00
end_date = (datetime.datetime.now() +
            datetime.timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S")

# Create hook connector from postgres_hook
postgres_hook = PostgresEsiosHook(ptgs_hook["user"],
                                  ptgs_hook["password"],
                                  ptgs_hook["conn_type"],
                                  ptgs_hook["host"],
                                  ptgs_hook["database"],
                                  ptgs_hook["port"])

for table in tables:
    esios_op = EsiosOperator(table, esios_hk["token"], esios_hk["base_url"])
    if table == "indicadores":
        df = esios_op.create_description_df()
        postgres_hook.load_df_esios(df, table, "replace")
        pass
    else:
        postgres_op = PostgresEsiosOperator(table,
                                            ptgs_hook["user"],
                                            ptgs_hook["password"],
                                            ptgs_hook["conn_type"],
                                            ptgs_hook["host"],
                                            ptgs_hook["database"],
                                            ptgs_hook["port"])
        # Create list of ranges
        info_table = esios_op.get_table_description(varbs["tb_folder"])
        start_date = postgres_op.get_max_timestamp(table,info_table["publicacion"])
        ranges = esios_op.date_range(start_date, end_date)
        for start, end in zip(ranges[:-1], ranges[1:]):
            end = (datetime.datetime.strptime(end, "%Y-%m-%dT%H:%M:%S") -
                   datetime.timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")
            df = esios_op.create_indicators_df(info_table,
                                               start,
                                               end)
            if df:
                df = esios_op.missing_control_df(df[0], df[1])
                """
                if table == "precios":
                    df = df[df["geo_id"] == 3]
                else:
                  pass
                """
                '''
                    # Create calculate fields
                    if table == "generacion_tiemporeal":
                        df = esios_op.calculate_columns_df(df,
                                                           varbs["gtr_rv"],
                                                           "gtreal_renovables")
                        df = esios_op.calculate_columns_df(df,
                                                           varbs["gtr_nrv"],
                                                           "gtreal_no_renovables")
                        df = esios_op.calculate_columns_df(df,
                                                           varbs["gtr_tc"],
                                                           "gtreal_tconexiones")
                        df = esios_op.calculate_columns_df(df,
                                                           varbs["gtr_ttl"],
                                                           "gtreal_total")
                    elif table == "generacion_medida":
                        df = esios_op.calculate_columns_df(df,
                                                           varbs["gm_term"],
                                                           "gmedida_termica")
                        df = esios_op.calculate_columns_df(df,
                                                           varbs["gm_rv"],
                                                           "gmedida_renovables")
                        df = esios_op.calculate_columns_df(df,
                                                           varbs["gm_nrv"],
                                                           "gmedida_no_renovables")
                        df = esios_op.calculate_columns_df(df,
                                                           varbs["gm_int"],
                                                           "gmedida_internacionales")
                        df = esios_op.calculate_columns_df(df,
                                                           varbs["gm_ttl"],
                                                           "gmedida_total")
                        df = esios_op.drop_columns_df(df, varbs["gm_ttl"])
                    else:
                        pass
                '''
                df = (df.groupby(['datetime',"geo_id","geo_name"])
                                 .mean().reset_index())    
                postgres_hook.load_df_esios(df, table)
                print("Load data from {} to {} in {}".format(start,
                                                             end,
                                                             table))
