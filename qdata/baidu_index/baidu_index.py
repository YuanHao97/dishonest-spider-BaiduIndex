from typing import List, Dict
import datetime
import json

from . import common
from qdata.errors import QdataError, ErrorCode

# ALL_KIND = ['all', 'pc', 'wise']
ALL_KIND = ['all']


def get_search_index(
    *,
    keywords_list: List[List[str]],
    start_date: str,
    end_date: str,
    cookies: str,
    area: int = 0,
    split_time: bool = True
):
    if len(keywords_list) > 5:
        raise QdataError(ErrorCode.KEYWORD_LIMITED)
    if split_time:
        time_range = common.get_time_range_list(start_date, end_date)
    else:
        time_range = [(datetime.datetime.strptime(start_date, '%Y-%m-%d'), datetime.datetime.strptime(end_date, '%Y-%m-%d'))]
    for start_date, end_date in time_range:
        print("{} time range: {} - {}".format(datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'), start_date, end_date))
        encrypt_json = common.get_encrypt_json(
            start_date=start_date,
            end_date=end_date,
            keywords=keywords_list,
            type='search',
            area=area,
            cookies=cookies
        )
        encrypt_datas = encrypt_json['data']['userIndexes']
        general_ratio = encrypt_json['data']['generalRatio']
        uniqid = encrypt_json['data']['uniqid']

        key = common.get_key(uniqid, cookies)
        for i, encrypt_data in enumerate(encrypt_datas):

            for kind in ALL_KIND:
                encrypt_data[kind]['data'] = common.decrypt_func(key, encrypt_data[kind]['data'])
                avg = general_ratio[i][kind]['avg']
            for formated_data in format_data(encrypt_data,area,avg):
                yield formated_data

def format_data(data: Dict,area,avg):
    """
        格式化堆在一起的数据
    """
    keyword = str(data['word'])
    start_date = datetime.datetime.strptime(data['all']['startDate'], '%Y-%m-%d')
    end_date = datetime.datetime.strptime(data['all']['endDate'], '%Y-%m-%d')
    date_list = []
    while start_date <= end_date:
        date_list.append(start_date)
        start_date += datetime.timedelta(days=1)

    for kind in ALL_KIND:
        index_datas = data[kind]['data']
        for i, cur_date in enumerate(date_list):
            try:
                index_data = index_datas[i]
            except IndexError:
                index_data = ''
            formated_data = {
                'keyword': [keyword_info['name'] for keyword_info in json.loads(keyword.replace('\'', '"'))],
                'type': kind,
                'date': cur_date.strftime('%Y-%m-%d'),
                'index': index_data if index_data else '0',
                'area': area,
                'avg': avg
            }
            yield formated_data
