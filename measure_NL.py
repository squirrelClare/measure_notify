"""
cron: 10 30 8 * * ?
new Env('量化--新低指标');
"""

import pandas as pd
from notify import pg_engine_finance, send
import datetime
if __name__ == '__main__':
    engine_finance_db = pg_engine_finance()
    start_time = '2022-01-01'
    end_time = datetime.datetime.now().strftime('%Y-%m-%d')
    feature_info = pd.read_sql_query("SELECT t.ts_code, t.trade_date, t.field, t.value FROM public.t_feature_numberic t where t.field='NL' and t.trade_date > '2022-01-01' order by t.trade_date", engine_finance_db)
    send("新高新低指标(NHNL)", feature_info.to_html(), "{}至{}期间每日新高新低指标".format(start_time, end_time))