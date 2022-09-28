"""
cron: 0 30 8 * * ?
new Env('量化--新高新低、新高、新低指标');
"""

import pandas as pd
from notify import pg_engine_finance, send
import datetime
if __name__ == '__main__':
    engine_finance_db = pg_engine_finance()
    # start_time = '2022-01-01'
    start_time = (datetime.datetime.now() - datetime.timedelta(days=40)).strftime('%Y-%m-%d')
    end_time = datetime.datetime.now().strftime('%Y-%m-%d')
    feature_info = pd.read_sql_query("SELECT t.trade_date, t.field, t.value FROM public.t_feature_numberic t where t.field in ('NL', 'NH', 'NHNL') and t.trade_date > '{0}'".format(start_time), engine_finance_db)
    #   长表转宽表
    feature_info_long = pd.pivot_table(feature_info, index=['trade_date'], columns='field', values='value').sort_index(ascending=False)[['NHNL', 'NH', 'NL']]
    send("新高新低、新高、新低指标(NHNL、NH、NL)", feature_info_long.to_html(), "{}至{}期间每日新高新低、新高、新低指标".format(start_time, end_time))
