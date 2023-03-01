"""
cron: 10 31 8 * * ?
new Env('量化--EMA65、EMA13的趋势斜率');
"""

import pandas as pd
from notify import pg_engine_finance, send
import datetime

if __name__ == '__main__':
    engine_finance_db = pg_engine_finance()
    # start_time = '2022-01-01'
    current_time = datetime.datetime.now().strftime('%Y-%m-%d')
    pre_10_day = (datetime.datetime.now() - datetime.timedelta(days=10)).strftime('%Y-%m-%d')

    # 获取最近一个交易日
    sql_cal_day = '''select * from t_trade_cal where cal_date>'{0}' and is_open=true order by cal_date '''.format(
        pre_10_day)
    cal_day_info = pd.read_sql_query(sql_cal_day, engine_finance_db)
    last_cal_day = cal_day_info['cal_date'].values[-1]

    #   抽取特征数据
    sql_feature_ema65_slope = '''select company, trade_date, field, value, a.ts_code as ts_code 
    from (select * from t_feature_numberic where field ='SLOPE_EMA_65' and trade_date ='{0}' and value > 0 limit 200) a left join t_tscode_company b on a.ts_code =b.ts_code order 
    by value desc '''.format(last_cal_day)
    feature_info_ema65_slope = pd.read_sql_query(sql_feature_ema65_slope, engine_finance_db)

    # ts_code_set = ','.join(['\'%s\''.format(e) for e in feature_info_ema65_slope['ts_code']])
    ts_code_set = ','.join(['\'{0}\''.format(e) for e in feature_info_ema65_slope['ts_code'].tolist()])
    # print(ts_code_set)
    # #   抽取特征数据
    # sql_feature_ema13_slope = '''select company, trade_date, field, value
    # from (select * from t_feature_numberic where field ='SLOPE_EMA_13' and trade_date ='{0}' and ts_code in ({1})) a left join t_tscode_company b on a.ts_code =b.ts_code order
    # by value'''.format(last_cal_day, ts_code_set)
    # feature_info_ema13_slope = pd.read_sql_query(sql_feature_ema13_slope, engine_finance_db)


    #   抽取特征数据
    sql_feature_multi = '''select company, field, value 
    from (select * from t_feature_numberic where field in ('SLOPE_EMA_13', 'CONFIDENCE_EMA_65', 'CONFIDENCE_EMA_13', 'TREND_EMA_65', 
    'TREND_EMA_13', 'CHANGE_TREND_EMA_65', 'CHANGE_TREND_EMA_13', 'DIFF_2_EMA_13', 'QUANTILE', 'ABOVE_MA200') and trade_date ='{0}' and ts_code in ({1})) a left join t_tscode_company b on a.ts_code =b.ts_code order 
    by value'''.format(last_cal_day, ts_code_set)
    feature_info_multi = pd.read_sql_query(sql_feature_multi, engine_finance_db)
    feature_concat = pd.concat([feature_info_ema65_slope, feature_info_multi]).reset_index(drop=True).pivot(index='company', columns='field', values='value')
    feature_concat['company'] = feature_concat.index
    feature_concat.reset_index(drop=True, inplace=True)
    # print(feature_info_multi)
    #   交易量排名
    sql_amount_rank = "select company, amount_rank, a.ts_code as ts_code, trade_date  from (select ts_code, trade_date, RANK() OVER (ORDER BY amount DESC) as amount_rank  from t_daily_info where trade_date ='{0}' and ts_code in ({1})) a left join t_tscode_company b on a.ts_code =b.ts_code" \
        .format(last_cal_day, ts_code_set)
    frame_amount_rank = pd.read_sql_query(sql_amount_rank, engine_finance_db)

    # feature_info = pd.merge(feature_info_multi, feature_info_ema65_slope, on=['company'])
    feature_info_amount_rank = pd.merge(feature_concat, frame_amount_rank, on='company').sort_values(by='SLOPE_EMA_13')
    feature_info_amount_rank.rename(columns={'company': '公司名', 'trade_date': '交易日期', 'SLOPE_EMA_13': 'EMA13斜率',
                                             'SLOPE_EMA_65': 'EMA65斜率', 'ts_code': '股票代码', 'amount_rank': '交易量排名',
                                             # 'CHANGE_TREND_EMA_13': 'EMA13_MK检验突变点个数', 'CHANGE_TREND_EMA_65': 'EMA65_MK检验突变点个数',
                                             # 'CONFIDENCE_EMA_13': 'EMA13_MK检验置信度', 'CONFIDENCE_EMA_65': 'EMA65_MK检验置信度',
                                             # 'TREND_EMA_13': 'EMA13_MK检验趋势', 'TREND_EMA_65': 'EMA65_MK检验趋势',
                                             'DIFF_2_EMA_13': 'EMA13二阶差分',
                                             'QUANTILE': "收盘价分位", 'ABOVE_MA200': '是否高于MA200'}, inplace=True)
    # print(feature_info_amount_rank)

    # send("趋势斜率", feature_info_amount_rank[['公司名', '股票代码', '交易日期', '交易量排名', 'EMA65斜率', 'EMA65_MK检验置信度',
    #                                        'EMA65_MK检验趋势', 'EMA65_MK检验突变点个数', 'EMA13斜率', 'EMA13二阶差分']].to_html(),
    #      "{}日趋势斜率，最多展示前一个交易日长期趋势斜率大于0的200家企业，长期趋势斜率为EMA65近20日斜率，短期趋势为EMA13近10日斜率;此外还展示当日交易量的排名信息。".format(current_time))

    send("趋势斜率", feature_info_amount_rank[['公司名', '股票代码', '交易日期', '交易量排名', 'EMA65斜率', 'EMA13斜率', 'EMA13二阶差分', '收盘价分位']].to_html(),
         "{}日趋势斜率，最多展示前一个交易日长期趋势斜率大于0的200家企业，长期趋势斜率为EMA65近20日斜率，短期趋势为EMA13近10日斜率;此外还展示当日交易量的排名信息。".format(current_time))
