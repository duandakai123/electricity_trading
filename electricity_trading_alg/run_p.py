#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/9/7 15:30
# @Author  : yry

import argparse
import logging
from electricity_trading_alg.api_ts import electricity_predict

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        filename='run_p.log',
                        format='[%(asctime)s]-%(thread)d-%(levelname)s(%(name)s): %(message)s - %(filename)s:%(lineno)d')
    parser = argparse.ArgumentParser(description='算法输入参数')
    parser.add_argument('predict_date', type=str, help='启动预报日期, eg: 20210826')
    parser.add_argument('predict_type', type=str, choices=['1st', '2nd', '3rd'], help='预报类型')
    parser.add_argument('user_ids', type=str,
                        help='用户id列表, eg: "{\"user_id_list\": [\"3201001159007\", \"3201001934370\"]}"')
    parser.add_argument('method', type=str, default='rm', choices=['rm', 'es'], help='预报方法')
    args = parser.parse_args()
    print(electricity_predict(args.predict_date, args.predict_type, eval(args.user_ids), args.method, is_write=False))
    # python run_p.py 20210726 1st "{\"user_id_list\": [\"3201001159007\", \"3201001934370\"]}" rm
