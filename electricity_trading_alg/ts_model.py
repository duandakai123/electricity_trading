#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/8/12 17:12
# @Author  : yry

import pandas as pd
from statsmodels.tsa.holtwinters import SimpleExpSmoothing


def rolling_mean_md(data_ser, window, negative_replace=0, zero_threshold=3):
    """
    移动平均模型, 根据序列值，返回基于移动平均方法预报的下一个值
    :param pd.Series data_ser: 输入的序列
    :param int window: 移动平均的窗口大小
    :param float negative_replace: 序列中负值的替换方法，0或nan
    :param int zero_threshold: 当序列中最近的zero_threshold值都为0时返回0
    :return: float
    """
    data_ser.dropna(inplace=True)
    data_ser[data_ser < 0] = negative_replace
    if (data_ser.count() == 0) or (data_ser[-zero_threshold:].sum() == 0):
        return 0
    if data_ser.count() < window:
        return data_ser.mean()
    return data_ser[-window:].rolling(window).mean().values[-1]


def exp_smoothing_md(data_ser, alpha, negative_replace=0, zero_threshold=3, minimum_items=4):
    """
    指数平滑模型, 根据序列值，返回基于指数平滑方法预报的下一个值
    :param pd.Series data_ser: 输入的序列
    :param float alpha: 指数平滑的系数
    :param float negative_replace: 序列中负值的替换方法，0或nan
    :param int zero_threshold: 当序列中最近的zero_threshold值都为0时返回0
    :param int minimum_items: 最少有效数据条数
    :return: float
    """
    data_ser.dropna(inplace=True)
    data_ser[data_ser < 0] = negative_replace
    if (data_ser.count() == 0) or (data_ser[-zero_threshold:].sum() == 0):
        return 0
    if data_ser.count() < minimum_items:
        return data_ser.mean()
    md = SimpleExpSmoothing(list(data_ser)).fit(smoothing_level=alpha, optimized=False)
    return md.forecast(1)[0]


def daily_predict(data_ser, predict_points):
    """
    预报月末的天级别数据
    :param pd.Series data_ser: 输入的序列
    :param int predict_points: 预报的点数
    :return: 返回完整的整月数据
    """
    data_ser.reset_index(drop=True, inplace=True)
    num = data_ser.count()
    for i in range(predict_points):
        data_ser[num + i] = rolling_mean_md(data_ser.copy(), window=8)
    return data_ser


def accurate_user_daily_predict(data_daily_df, days_of_month):
    """
    对所有准确户月末天级别的用电量进行批量预报
    :param pd.DateFrame data_daily_df:输入数据，已知的用电量数据
    :param int days_of_month:预报月份的总天数
    :return:
    """
    data_p = pd.DataFrame(columns=range(1, days_of_month + 1))
    predict_days = days_of_month - data_daily_df.shape[1]
    for idx in data_daily_df.index:
        data_p.loc[idx] = daily_predict(data_daily_df.loc[idx], predict_days).values
    return data_p.sum(axis=1)


def monthly_predict(data_mon, method='rm', window=2, alpha=0.5):
    """
    月度预报模型
    :param pd.DataFrame data_mon:月度数据
    :param str method:预报方法（rm， es）
    :param int window:滑动窗口大小
    :param float alpha:移动平均系数
    :return:pd.Series
    """
    data_predict = pd.Series()
    for idx in data_mon.index:
        if method == 'es':
            data_predict[idx] = exp_smoothing_md(data_mon.loc[idx], alpha=alpha)
        else:
            data_predict[idx] = rolling_mean_md(data_mon.loc[idx], window=window)
    return data_predict
