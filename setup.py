# encoding: utf-8
from setuptools import setup, find_packages

import electricity_trading_alg

setup(name='electricity_trading',
      version=electricity_trading_alg.__version__,
      packages=find_packages(),
      author='renyu.yuan',
      python_requires='>=3.6',
      platforms='any',
      install_requires=[
          "pandas",
          "numpy",
          'requests',
          'python-dateutil',
          'statsmodels',
          'flask',
          'retry'
      ]
      )
