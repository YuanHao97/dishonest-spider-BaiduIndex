from typing import List, Dict
import datetime
import json

from qdata.errors import QdataError, ErrorCode
from . import common


def get_news_index(
    *,
    keywords_list: List[List[str]],
    start_date: str,
    end_date: str,
    cookies: str,
    area: int = 0
):
    return get_extended_index(
        keywords_list=keywords_list,
        start_date=start_date,
        end_date=end_date,
        cookies=cookies,
        area=area,
        type='news'
    )


def get_feed_index(
    *,
    keywords_list: List[List[str]],
    start_date: str,
    end_date: str,
    cookies: str,
    area: int = 0,
    split_time: bool = True
):
    return get_extended_index(
        keywords_list=keywords_list,
        start_date=start_date,
        end_date=end_date,
        cookies=cookies,
        area=area,
        type='feed',
        split_time=split_time
    )


def get_extended_index(
    *,
    keywords_list: List[List[str]],
    start_date: str,
    end_date: str,
    cookies: str,
    area: int,
    type: str,
    split_time: bool = True
):
    if len(keywords_list) > 5:
        raise QdataError(ErrorCode.KEYWORD_LIMITED)
    if split_time:
        time_range = common.get_time_range_list(start_date, end_date)
    else:
        time_range = [(datetime.datetime.strptime(start_date, '%Y-%m-%d'), datetime.datetime.strptime(end_date, '%Y-%m-%d'))]
    for start_date, end_date in time_range:
        encrypt_json = common.get_encrypt_json(
            start_date=start_date,
            end_date=end_date,
            keywords=keywords_list,
            type=type,
            area=area,
            cookies=cookies
        )
        encrypt_datas = encrypt_json['data']['index']
        uniqid = encrypt_json['data']['uniqid']

        key = common.get_key(uniqid, cookies)
        for encrypt_data in encrypt_datas:
            encrypt_data['data'] = common.decrypt_func(key, encrypt_data['data'])
            avg = encrypt_data['generalRatio']['avg']
            for formated_data in format_data(encrypt_data, area,avg):
                formated_data['type'] = type
                yield formated_data


def format_data(data: Dict, area,avg):
    keyword = str(data['key'])
    start_date = datetime.datetime.strptime(data['startDate'], '%Y-%m-%d')
    end_date = datetime.datetime.strptime(data['endDate'], '%Y-%m-%d')
    date_list = []
    while start_date <= end_date:
        date_list.append(start_date)
        start_date += datetime.timedelta(days=1)

    index_datas = data['data']
    for i, cur_date in enumerate(date_list):
        try:
            index_data = index_datas[i]
        except IndexError:
            index_data = ''
        formated_data = {
            'keyword': [keyword_info['name'] for keyword_info in json.loads(keyword.replace('\'', '"'))],
            'date': cur_date.strftime('%Y-%m-%d'),
            'index': index_data if index_data else '0',
            'area': area,
            'avg': avg
        }
        yield formated_data
