# python unitapi/test.py

import requests
from loguru import logger
import pandas as pd
import json
import jwt
from datetime import datetime
import time
from pprint import pprint
import sys
sys.path.append('/Users/igorigor/VS code/Python work scripts/')
# sys.path.append('/home/pavelmalevin/regular_loadings')
from test_modules import *

token = 'some_token'
base = 'http://127.0.0.1:5000/api/v1'

headers = {
    'Token': token,
    # 'Content-Type': 'application/json'
}

df = print_post(
    base + '/clc/production/actions/format_estimation_json_for_print',
    headers=headers,
    json={
        'est_id': 10
    }
)

pprint(df.json())



