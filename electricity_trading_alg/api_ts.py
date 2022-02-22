#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/8/30 17:49
# @Author  : yry
# import sys
# import time
import json
import logging
import calendar
from multiprocessing import Process
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from electricity_trading_alg.ts_model import *
from electricity_trading_alg.data_api import get_user_list, get_fact_data, put_forecast_data, put_forecast_data_agg, \
    get_month_end_data, get_monthly_data_tc

logger = logging.getLogger(__name__)


class ElectricityPredict(object):
    def __init__(self, predict_date, predict_type, method='rm'):
        """
        电量预报
        :param str predict_date: 预报启动的日期，需传入年、月、日信息，为长度为8的字符串，例如：‘20210826’
        :param str predict_type: 预报类型，由于每个月份需要做三次预报，所以该字段的参数包括：'1st', '2nd', '3rd', '1st'代表月前预报，
        后两次是月内预报
        :param str method: 月度预报方法，目前有移动平均（'rm'）、指数平滑('es'）
        """
        self.predict_date = datetime.strptime(predict_date, '%Y%m%d')
        self.predict_type = predict_type
        self.method = method
        self.is_holiday_opt = True
        self.is_temperature_opt = True
        self.days_mon = calendar.monthrange(self.predict_date.year, self.predict_date.month)[1]
        self.accurate_users = get_user_list('accurate_user_list', self.predict_date, '江苏省')
        self.inaccurate_users = get_user_list('inaccurate_user_list', self.predict_date, '江苏省')
        self.accurate_users_p = None
        self.inaccurate_users_p = None
        self.all_users_p = None
        self.sum_res = None
        self.data_divide_num = 100

    def accurate_users_predict(self):
        """准确户预报"""
        logger.info('start prediction for accurate users')
        logger.info('loading accurate users historical data')
        data_his_d = get_fact_data(self.accurate_users, 'electricity', self.predict_date.replace(day=1),
                                   self.predict_date - timedelta(days=1), 'D', self.data_divide_num)
        self.accurate_users_p = accurate_user_daily_predict(data_his_d, self.days_mon)
        if self.predict_type == '1st':  # 月前预报
            acc_users_data_m = get_monthly_data_tc(self.accurate_users, datetime(2021, 3, 1),
                                                   self.predict_date.replace(day=1) - timedelta(days=1),
                                                   self.data_divide_num)
            acc_users_data_m['present_mon'] = self.accurate_users_p.values
            self.accurate_users_p = monthly_predict(acc_users_data_m, method=self.method, alpha=0.5)
        logger.info('accurate users prediction finished')

    def inaccurate_users_predict(self):
        """非准确户预报"""
        logger.info('start prediction for inaccurate users')
        if self.predict_type == '3rd':
            logger.info('loading inaccurate users month-end data')
            month_end_data = get_month_end_data(self.inaccurate_users, self.predict_date, self.data_divide_num)
            res_p1 = month_end_data * self.days_mon / (self.days_mon - 2)
            users_idx_2 = list(set(self.inaccurate_users) - set(month_end_data.index))
            if len(users_idx_2) != 0:
                logger.info('loading inaccurate users historical data')
                data_his = get_monthly_data_tc(users_idx_2, datetime(2021, 3, 1),
                                               self.predict_date.replace(day=1) - timedelta(days=1))
                res_p2 = monthly_predict(data_his, method=self.method, alpha=0.1)
            self.inaccurate_users_p = res_p1 if (len(users_idx_2) == 0) else pd.concat([res_p1, res_p2])
        else:
            logger.info('loading inaccurate users historical data')
            data_his = get_monthly_data_tc(self.inaccurate_users, datetime(2021, 3, 1),
                                           self.predict_date.replace(day=1) - timedelta(days=1),
                                           self.data_divide_num)
            self.inaccurate_users_p = monthly_predict(data_his, method=self.method, alpha=0.1)

    def predict_res_summary(self):
        """预报结果汇总"""
        self.all_users_p = pd.concat([self.accurate_users_p, self.inaccurate_users_p])
        self.sum_res = {'all_users': self.all_users_p.sum(), 'accurate_users': self.accurate_users_p.sum(),
                        'inaccurate_users': self.inaccurate_users_p.sum()}

    def write_predict_res(self):
        """将预报结果写入数据库"""
        logger.info(f'start to writer the results')
        write_metrics = f'forecast_electricity_{self.predict_type}'
        write_dt = self.predict_date.replace(day=1) + relativedelta(
            months=1) if self.predict_type == '1st' else self.predict_date.replace(day=1)
        for idx in self.all_users_p.index:
            data_list = [str(round(self.all_users_p[idx], 3))]
            put_forecast_data(str(idx), write_metrics, data_list, write_dt, write_dt)
        put_forecast_data_agg('accurate_users', write_metrics, [str(round(self.sum_res['accurate_users'], 3))],
                              write_dt,
                              write_dt)
        put_forecast_data_agg('inaccurate_users', write_metrics, [str(round(self.sum_res['inaccurate_users'], 3))],
                              write_dt, write_dt)
        put_forecast_data_agg('all_users', write_metrics, [str(round(self.sum_res['all_users'], 3))], write_dt,
                              write_dt)

    def temperature_opt_md(self):
        cf_dict = {'1st': {1: 1, 2: 1, 3: 1, 4: 0.98, 5: 1.04, 6: 1.03, 7: 1.07, 8: 1.01, 9: 0.93, 10: 0.98, 11: 0.99,
                           12: 1.02},
                   '2nd': {1: 1, 2: 1, 3: 1, 4: 0.99, 5: 1.02, 6: 1.02, 7: 1.04, 8: 1.03, 9: 0.93, 10: 0.98, 11: 0.99,
                           12: 1.01},
                   '3rd': {1: 1, 2: 1, 3: 1, 4: 1, 5: 1.02, 6: 1.01, 7: 1.05, 8: 1.03, 9: 1, 10: 0.99, 11: 1, 12: 1.01}}
        predict_mon = self.predict_date.month + 1 if self.predict_type == '1st' else self.predict_date.month
        correction_factor = cf_dict[self.predict_type][predict_mon]
        # print(correction_factor)
        if 0.9 < correction_factor < 1.1:
            self.all_users_p *= correction_factor
            self.accurate_users_p *= correction_factor
            self.inaccurate_users_p *= correction_factor
            self.sum_res.update((k, v * correction_factor) for k, v in self.sum_res.items())


def electricity_predict(predict_date, predict_type, user_ids, method='rm'):
    """
    电量预报
    :param str predict_date: 预报启动的日期，需传入年、月、日信息，为长度为8的字符串，例如：‘20210826’
    :param str predict_type: 预报类型，由于每个月份需要做三次预报，所以该字段的参数包括：'1st', '2nd', '3rd', '1st'代表月前预报，
    后两次是月内预报
    :param dict user_ids: 月需要输出用户级结果的用户id列表，格式：{'user_id_list': ['3201001159007', '3201001934370']}
    :param str method: 月度预报方法，目前有移动平均（'rm'）、指数平滑('es'）
    :return:
    """
    logger.info("*" * 100)
    logger.info('prediction model initialization')
    ep = ElectricityPredict(predict_date, predict_type, method)
    p1 = Process(target=ep.accurate_users_predict(), args=('process_name1',))
    p2 = Process(target=ep.inaccurate_users_predict(), args=('process_name2',))
    # ep.accurate_users_predict()
    p1.start()
    # ep.inaccurate_users_predict()
    p2.start()
    p1.join()
    p2.join()
    ep.predict_res_summary()
    ep.temperature_opt_md()
    logger.info(f'prediction is completed')
    # p3 = Process(target=ep.write_predict_res())
    # p3.start()
    user_res = ep.all_users_p.reindex(user_ids['user_id_list']).to_json()
    logger.info(f'finished successfully, start to output the results')
    return json.dumps(ep.sum_res), user_res
