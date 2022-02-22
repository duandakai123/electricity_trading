#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/8/27 11:34
# @Author  : yry

import logging
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from retry import retry

logger = logging.getLogger(__name__)

url_user_list = 'http://10.0.24.3:40884/ecp/ecpCompanyInfo/readUser'
url_fact_data_org = 'http://10.0.24.3:40884/ecp/ecpAccurateElectric/readElectrics'
url_fact_data_clean = 'http://10.0.24.3:40884/ecp/ecpElectricClean/insertClean'
url_forecast_data_read = 'http://10.0.24.3:40884/ecp/ecpElectricCollect/readElectrics'
url_forecast_data_write = 'http://10.0.24.3:40884/ecp/ecpElectricCollect/insertElectric'
url_month_end_fact_data = 'http://10.0.24.3:40884/ecp/ecpElectricSummary/readElectrics'
url_monthly_data_tc = 'http://10.0.24.3:40884/ecp/ecpElectricSummary/readSumElectrics'
headers = {'content-type': 'application/json'}


@retry(tries=5, delay=10)
def restful_post_json(url, body=None, params=None, timeout=60):
    resp = requests.post(url, headers=headers, json=body, params=params, timeout=timeout)
    if resp.text == '访问超时，请稍后再试!':
        logger.error(resp.text)
    return resp.json() if resp.content else None


def get_user_list(metrics, date_time, province, is_only_id=True):
    """
    获取用户列表
    :param str metrics: 用户列表类别，包括：user_list， accurate_user_list， inaccurate_user_list
    :param datetime date_time:获取用户列表对应的日期，需要用到年、月的信息
    :param str province: 用户对应的省份
    :param bool is_only_id: 是否只需返回用户id，True为只返回id，False返回用户id、用户名
    :return:list or pd.Series
    """
    resp = restful_post_json(url_user_list,
                             body={'metrics': metrics, 'datetime': date_time.strftime('%Y-%m-%d %H:%M:%S')[:7],
                                   'province': province})
    if is_only_id:
        print(1)
        return [r['user_id'] for r in resp]
    else:
        print(1)
        return pd.Series([r['user_name'] for r in resp], index=[r['user_id'] for r in resp], dtype=str)


def get_fact_data(user_id_list, metrics, start_time, end_time, freq, divide_num=1):
    """
    获取实际数据, 数据长度默认左闭右闭
    :param list user_id_list: 用户id列表
    :param str metrics: 获取实际数据字段名，可以是：'electricity', 'power'
    :param datetime start_time:数据开始时间
    :param datetime end_time:数据结束时间
    :param str freq:数据分辨率
    :param int divide_num:数据分割份数
    :return: pd.DataFrame
    """
    start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
    str_len = 10 if (freq == 'D') else 7
    start_time_s = start_time[:str_len]
    end_time_s = end_time[:str_len]

    divide_avg = len(user_id_list) // divide_num
    resp_total = []
    for num in range(divide_num):
        if num == divide_num - 1:
            sub_list = user_id_list[num * divide_avg:]
        else:
            sub_list = user_id_list[num * divide_avg:(num + 1) * divide_avg]
        body = {'user_id_list': sub_list, 'metrics': metrics, 'start_time': start_time_s,
                'end_time': end_time_s, 'freq': freq}
        # print(body)
        resp_total = resp_total + restful_post_json(url_fact_data_org, body=body)
    res_df = pd.DataFrame(index=[d.strftime('%Y-%m-%d %H:%M:%S')[:str_len] for d in
                                 pd.date_range(start_time, periods=len(resp_total[0][user_id_list[0]]), freq=freq)])
    for d in resp_total:
        for k, v in d.items():
            res_df[k] = v
    return res_df.astype('float').T


def get_history_fact_data(start_time, end_time, data_source='tc'):
    """
    获取历史上一段时间段的实际电量数据, 数据长度默认左闭右开
    :param datetime start_time:数据开始时间
    :param datetime end_time:数据结束时间
    :param str data_source:数据来源，包括两种情况，分别来自交易中心（‘tc’）和平台电表采集（'platform'）
    :return: pd.DataFrame
    """
    month_list = [d.strftime('%Y-%m-%d %H:%M:%S')[:7] for d in pd.date_range(start_time, end_time, freq='M')]
    sum_type_dict = {'all_users': 'user_list', 'accurate_users': 'accurate_user_list',
                     'inaccurate_users': 'inaccurate_user_list'}
    res_df = pd.DataFrame(index=month_list, columns=sum_type_dict.keys())
    writer = pd.ExcelWriter(f'fact_res_users.xlsx')
    for sum_type in sum_type_dict.keys():
        for mon in month_list:
            mon_dt = datetime.strptime(mon, '%Y-%m')
            user_id_list = get_user_list(sum_type_dict[sum_type], mon_dt, '江苏省', is_only_id=True)
            if data_source == 'tc':
                data = get_monthly_data_tc(user_id_list, mon_dt, mon_dt, divide_num=50)
            else:
                data = get_fact_data(user_id_list, 'electricity', mon_dt, mon_dt, 'M', divide_num=50)
            data.to_excel(writer, sheet_name=f'{mon}_{sum_type}')
            res_df.loc[mon, sum_type] = data.sum().values[0]
    writer.save()
    return res_df


def get_month_end_data(user_id_list, date_mon, divide_num=1):
    """
    获取月底交易中心反馈的月度数据(预抄核), 该数据覆盖用户相应月份倒数第二天之前的用电量总和
    :param list user_id_list: 用户id列表
    :param datetime date_mon: 获取数据的月份
    :param int divide_num:数据分割份数
    :return: pd.Series
    """
    date_mon_s = date_mon.strftime('%Y-%m-%d %H:%M:%S')[:7]
    divide_avg = len(user_id_list) // divide_num
    resp_total = []
    for num in range(divide_num):
        if num == divide_num - 1:
            sub_list = user_id_list[num * divide_avg:]
        else:
            sub_list = user_id_list[num * divide_avg:(num + 1) * divide_avg]
        body = {'user_id_list': sub_list, 'datetime': date_mon_s}
        # print(body)
        resp_total = resp_total + restful_post_json(url_month_end_fact_data, body=body)
    res_s = pd.Series()
    for d in resp_total:
        for k, v in d.items():
            res_s[k] = v['electricNum']
    return res_s.astype('float')


def get_monthly_data_tc(user_id_list, start_time, end_time, divide_num=1):
    """
    获取交易中心月度级别数据
    :param list user_id_list: 用户id列表
    :param datetime start_time:数据开始时间
    :param datetime end_time:数据结束时间
    :param int divide_num:数据分割份数
    :return: pd.DataFrame
    """
    month_list = [d.strftime('%Y-%m-%d %H:%M:%S')[:7] for d in
                  pd.date_range(start_time.replace(day=1), end_time.replace(day=1) + relativedelta(months=1), freq='M')]
    data_df = pd.DataFrame(index=month_list, columns=user_id_list)
    divide_avg = len(user_id_list) // divide_num
    for date_mon in month_list:
        resp_total = []
        for num in range(divide_num):
            if num == divide_num - 1:
                sub_list = user_id_list[num * divide_avg:]
            else:
                sub_list = user_id_list[num * divide_avg:(num + 1) * divide_avg]
            body = {'user_id_list': sub_list, 'datetime': date_mon}
            resp_total = resp_total + restful_post_json(url_monthly_data_tc, body=body)
        res_s = pd.Series()
        for d in resp_total:
            for k, v in d.items():
                res_s[k] = v['electricNum']
        data_df.loc[date_mon] = res_s
    return data_df.astype('float').T


def get_forecast_data(user_id_list, metrics, start_time, end_time):
    """
    获取用户级预报数据, 数据长度默认左闭右闭
    :param list user_id_list: 用户id列表
    :param str metrics: 获取实际数据字段名，可以是：'forecast_electricity_1st', 'forecast_electricity_2nd',
    'forecast_electricity_3rd', 'forecast_power'
    :param datetime start_time:数据开始时间
    :param datetime end_time:数据结束时间
    :return: pd.DataFrame
    """
    start_time_s = start_time.strftime('%Y-%m-%d %H:%M:%S')[:7]
    end_time_s = end_time.strftime('%Y-%m-%d %H:%M:%S')[:7]
    body = {'user_id_list': user_id_list, 'metrics': metrics, 'start_time': start_time_s, 'end_time': end_time_s}
    resp = restful_post_json(url_forecast_data_read, body=body)
    res_df = pd.DataFrame(index=[d.strftime('%Y-%m-%d %H:%M:%S')[:7] for d in
                                 pd.date_range(start_time, periods=len(resp[user_id_list[0]]), freq='M')])
    for k, v in resp.items():
        res_df[k] = v
    return res_df


def get_forecast_data_agg(sum_type, metrics, start_time, end_time):
    """
    获取汇总预报数据, 数据长度默认左闭右闭
    :param str sum_type: 汇总的形式，包括：'all_users', 'accurate_users', 'inaccurate_users'
    :param str metrics: 获取实际数据字段名，可以是：'forecast_electricity_1st', 'forecast_electricity_2nd',
    'forecast_electricity_3rd', 'forecast_power'
    :param datetime start_time:数据开始时间
    :param datetime end_time:数据结束时间
    :return: pd.DataFrame
    """
    start_time_s = start_time.strftime('%Y-%m-%d %H:%M:%S')[:7]
    end_time_s = end_time.strftime('%Y-%m-%d %H:%M:%S')[:7]
    body = {'sum_type': sum_type, 'metrics': metrics, 'start_time': start_time_s, 'end_time': end_time_s}
    resp = restful_post_json(url_forecast_data_read, body=body)
    # print(resp)
    res_df = pd.DataFrame(index=[d.strftime('%Y-%m-%d %H:%M:%S')[:7] for d in
                                 pd.date_range(start_time, periods=len(resp[sum_type]), freq='M')])
    for k, v in resp.items():
        res_df[k] = v
    return res_df


def get_history_forecast_data(start_time, end_time):
    """
    获取历史上一段时间段的预报结果, 数据长度默认左闭右开
    :param datetime start_time:数据开始时间
    :param datetime end_time:数据结束时间
    :return: pd.DataFrame
    """
    month_list = [d.strftime('%Y-%m-%d %H:%M:%S')[:7] for d in pd.date_range(start_time, end_time, freq='M')]
    sum_type_list = ['all_users', 'accurate_users', 'inaccurate_users']
    metrics_list = ['forecast_electricity_1st', 'forecast_electricity_2nd', 'forecast_electricity_3rd']
    res_df = pd.DataFrame(index=month_list, columns=pd.MultiIndex.from_product([metrics_list, sum_type_list]))
    for metric in metrics_list:
        for sum_type in sum_type_list:
            res_df.loc[:, (metric, sum_type)] = get_forecast_data_agg(sum_type, metric, start_time, end_time).iloc[:-1, 0].values
    return res_df


def put_forecast_data(user_id, metrics, data, start_time, end_time):
    """
    写入用户级预报数据, 数据长度默认左闭右闭
    :param str user_id: 用户id
    :param str metrics: 获取实际数据字段名，可以是：'forecast_electricity_1st', 'forecast_electricity_2nd',
    'forecast_electricity_3rd', 'forecast_power'
    :param list data:写入的数据
    :param datetime start_time:数据开始时间
    :param datetime end_time:数据结束时间
    :return: pd.DataFrame
    """
    start_time_s = start_time.strftime('%Y-%m-%d %H:%M:%S')[:7]
    end_time_s = end_time.strftime('%Y-%m-%d %H:%M:%S')[:7]
    body = {'user_id': user_id, 'metrics': metrics, 'data': data, 'start_time': start_time_s, 'end_time': end_time_s}
    requests.post(url_forecast_data_write, headers=headers, json=body)


def put_forecast_data_agg(sum_type, metrics, data, start_time, end_time):
    """
    写入汇总预报数据, 数据长度默认左闭右闭
    :param str sum_type: 汇总的形式，包括：'all_users', 'accurate_users', 'inaccurate_users'
    :param str metrics: 获取实际数据字段名，可以是：'forecast_electricity_1st', 'forecast_electricity_2nd',
    'forecast_electricity_3rd', 'forecast_power'
    :param list data:写入的数据
    :param datetime start_time:数据开始时间
    :param datetime end_time:数据结束时间
    :return: pd.DataFrame
    """
    start_time_s = start_time.strftime('%Y-%m-%d %H:%M:%S')[:7]
    end_time_s = end_time.strftime('%Y-%m-%d %H:%M:%S')[:7]
    body = {'sum_type': sum_type, 'metrics': metrics, 'data': data, 'start_time': start_time_s, 'end_time': end_time_s}
    # print(body)
    requests.post(url_forecast_data_write, headers=headers, json=body)


if __name__ == '__main__':
    print(len(get_user_list('user_list', datetime(2021, 11, 1), '江苏省')))
    # print(get_user_list('inaccurate_user_list', datetime(2021, 7, 1), '江苏省'))
    # print(get_fact_data(['3201600030780'], 'electricity', datetime(2021, 11, 1), datetime(2021, 11, 30), 'D'))
    # print(get_monthly_data_tc(['3201600030780', '3200152577756'], datetime(2021, 10, 1), datetime(2021, 11, 30), divide_num=1))
    # in_acc_lists = list(get_user_list('inaccurate_user_list', datetime(2021, 7, 1), '江苏省').index)
    # in_acc_data = get_month_end_data(in_acc_lists, datetime(2021, 7, 1))
    # print(in_acc_data)
    # print(get_month_end_data(['3200152577756'], datetime(2021, 7, 1)))
    # print(get_month_end_data(['3203001531715'], datetime(2021, 7, 1)))
    # print(get_fact_data(['3208837196323', '3200332344414'],
    #                     'electricity', datetime(2021, 6, 1), datetime(2021, 7, 31),
    #                     'D'))
    # print(get_fact_data(['3201001159007'], 'power', datetime(2021, 7, 1), datetime(2021, 7, 2), '15min'))

    # put_forecast_data('3200332344414', 'forecast_electricity_3rd', ['1060.6336'], datetime(2020, 8, 1),
    #                   datetime(2020, 8, 1))
    # print(get_forecast_data(['3206700097536', '3200332344414'], 'forecast_electricity_3rd', datetime(2020, 8, 1),
    #                         datetime(2020, 8, 31)))
    # print(get_forecast_data(['3200100276056'], 'forecast_electricity_1st', datetime(2021, 8, 1),
    #                         datetime(2021, 8, 1)))
    # put_forecast_data_agg('accurate_users', 'forecast_electricity_1st', ['1060.6336'], datetime(2021, 8, 1),
    #                       datetime(2021, 8, 1))
    # print(get_forecast_data_agg('accurate_users', 'forecast_electricity_1st', datetime(2021, 8, 1), datetime(2021, 8, 1)))
    # print(get_forecast_data_agg('inaccurate_users', 'forecast_electricity_1st', datetime(2021, 8, 1),
    #                             datetime(2021, 8, 1)))
    # print(get_forecast_data_agg('inaccurate_users', 'forecast_electricity_1st', datetime(2021, 8, 1),
    #                             datetime(2021, 8, 1)))
    # res = get_history_forecast_data(datetime(2021, 7, 1), datetime(2021, 12, 1))
    # res.to_excel('forecast_res.xlsx')

    # res = get_history_fact_data(datetime(2021, 9, 1), datetime(2021, 10, 1))  # todo
    # res.to_excel('fact_res_tc.xlsx')

    # acc_users = get_user_list('accurate_user_list', datetime(2021, 8, 1), '江苏省')
    # acc_users_data = get_fact_data(acc_users, 'electricity', datetime(2021, 1, 1), datetime(2021, 8, 31), 'D', divide_num=10)
    # acc_users_data.T.to_csv('acc_users_data_2021.8.31.csv')
