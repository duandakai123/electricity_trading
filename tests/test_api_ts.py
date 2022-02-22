#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/8/31 16:09
# @Author  : yry
import json
import logging
from electricity_trading_alg.api_ts import electricity_predict

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        filename='run_test.log',
                        format='[%(asctime)s]-%(thread)d-%(levelname)s(%(name)s): %(message)s - %(filename)s:%(lineno)d')
    print(electricity_predict('20211230', '3rd', {'user_id_list': ['3201001159007']}, method='rm'))
    # electricity_predict('20210726', '1st', is_write=True)
