#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/9/7 18:48
# @Author  : yry
import logging
from flask import Flask, request, jsonify
from electricity_trading_alg.api_ts import electricity_predict

app = Flask(__name__)
res = []


@app.route('/forecast_results')
def get_massage():
    return jsonify(res)


@app.route('/forecast_results', methods=['POST'])
def create_massage():
    alg_input = request.get_json()
    res_alg = electricity_predict(**alg_input)
    while len(res) != 0:
        res.pop(0)
    res.extend(res_alg)
    return jsonify(res)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        filename='run_flask.log',
                        format='[%(asctime)s]-%(thread)d-%(levelname)s(%(name)s): %(message)s - %(filename)s:%(lineno)d')
    app.run(host='0.0.0.0', port=40884)
