# Несколько модулей, с которыми удобно тестировать API вручную

from loguru import logger
import pandas as pd
import requests
import jwt
from datetime import datetime


def convert_to_df_decorator(func):
    def wrapper(*args, **kwargs):
        try:
            response = func(*args, **kwargs)
            logger.info(f"\nКод ответа: {response.status_code}")
            df = pd.DataFrame(response.json())
            return df
        except Exception:
            logger.exception('Ошибка при получении датафрейма')
        return response
    return wrapper


def print_ans_decorator(func):
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        logger.info(f"\nКод ответа: {response.status_code}\nСообщение: {response.text}")
        return response
    return wrapper

@print_ans_decorator
def print_get(*args, **kwargs):
    ans = requests.get(*args, **kwargs)
    return ans

@print_ans_decorator
def print_post(*args, **kwargs):
    ans = requests.post(*args, **kwargs)
    return ans

@print_ans_decorator
def print_put(*args, **kwargs):
    ans = requests.put(*args, **kwargs)
    return ans

@print_ans_decorator
def print_delete(*args, **kwargs):
    ans = requests.delete(*args, **kwargs)
    return ans


@convert_to_df_decorator
def df_get(*args, **kwargs):
    ans = requests.get(*args, **kwargs)
    return ans

@convert_to_df_decorator
def df_post(*args, **kwargs):
    ans = requests.post(*args, **kwargs)
    return ans
