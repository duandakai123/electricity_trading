FROM harbor.heilansc.com/library/python:3.7.11-bullseye
LABEL author="zhoukaifeng@heilansc.com"

WORKDIR /app
COPY . .
RUN pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple && python setup.py build && python setup.py install
RUN mv electricity_trading_alg /tmp/ && rm -rf /app/* && mv /tmp/electricity_trading_alg /app/

EXPOSE 40884

CMD [ "python","electricity_trading_alg/run_flask.py" ]
