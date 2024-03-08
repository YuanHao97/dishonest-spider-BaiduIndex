"""
百度指数数据获取最佳实践
此脚本完成
1. 清洗关键词
2. 发更少请求获取更多的数据
3. 请求容错
4. 容错后并保留当前已经请求过的数据，并print已请求过的keywords
"""
from queue import Queue
from typing import Dict, List
import datetime
import traceback
import time

import pandas as pd
from qdata.baidu_index import get_search_index
from qdata.baidu_index import get_feed_index
from qdata.baidu_index.common import check_keywords_exists, split_keywords

cookies = "BAIDUID=C94E71F42A6F790D29D970728B032CE8:FG=1; BAIDUID_BFESS=C94E71F42A6F790D29D970728B032CE8:FG=1; BDUSS=FPN3V1dnY3bjFTWi13cmlCclFYUnY1MzF2UXp-LWJsWEdKZlc5YjdzTnVPeEptRVFBQUFBJCQAAAAAAAAAAAEAAABQWgVBYmVsaWV2ZTg5NAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG6u6mVuruplVT; BDUSS_BFESS=FPN3V1dnY3bjFTWi13cmlCclFYUnY1MzF2UXp-LWJsWEdKZlc5YjdzTnVPeEptRVFBQUFBJCQAAAAAAAAAAAEAAABQWgVBYmVsaWV2ZTg5NAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG6u6mVuruplVT; BIDUPSID=C94E71F42A6F790D29D970728B032CE8; H_PS_PSSID=39662_40207_40212_40217_40294_40291_40287_40285_40079_40364_40352_40373_40367_40317; PSTM=1709878894; bdindexid=baro17vijkbfemvour9sjf5li2; PTOKEN=c74f1608e72a402842aadb7f9cfbc521; PTOKEN_BFESS=c74f1608e72a402842aadb7f9cfbc521; STOKEN=6ede54cc33e9ae5ae4ed32c1019f87a7b731beec6238a69fd3dd37e52904b2a7; STOKEN_BFESS=6ede54cc33e9ae5ae4ed32c1019f87a7b731beec6238a69fd3dd37e52904b2a7; UBI=fi_PncwhpxZ%7ETaJcxFmvzU7ni8BBW0NmP2Y; UBI_BFESS=fi_PncwhpxZ%7ETaJcxFmvzU7ni8BBW0NmP2Y; __yjs_st=2_YWNjY2IyNzk5MTY5MWExNTU4NmQ1MTczNTM3YmQwZjUzOGJlODkxMjc5ZmQ0YmU2NDg4ZmI1ZGMxMjY2OTNlYzRjMTQ5ZGFhOTBkMzBlNTgzZGJiNTczZTg4NDk4YzM5NTFkYjNhZmRhNGViYmZjNzc0YjNiMDM4MTgxYjM4NTAyMDE1ZTgxOTE1N2E0NDliODI5ZWQ0NGE4YWY2YmMzYTM3MzI2N2Y0YmRiNjc0YTk5NDYzMWJkMzI5M2RiMGI2XzdfYTk3NjRjYzc="
index_type = ["search", "feed"]
area_ids = [0]
start_date = "2022-11-01"
end_date = "2023-01-01"


def get_clear_keywords_list(keywords_list: List[List[str]]) -> List[List[str]]:
    q = Queue(-1)

    cur_keywords_list = []
    for keywords in keywords_list:
        cur_keywords_list.extend(keywords)
    
    # 先找到所有未收录的关键词
    for start in range(0, len(cur_keywords_list), 15):
        q.put(cur_keywords_list[start:start+15])
    
    not_exist_keyword_set = set()
    while not q.empty():
        keywords = q.get()
        # try:
            # check_result = check_keywords_exists(keywords, cookies)
            # time.sleep(5)
        # except:
        #     traceback.print_exc()
        #     q.put(keywords)
        #     time.sleep(90)

        # for keyword in check_result["not_exists_keywords"]:
        #     not_exist_keyword_set.add(keyword)
    
    # 在原有的keywords_list拎出没有收录的关键词
    new_keywords_list = []
    for keywords in keywords_list:
        not_exists_count = len([None for keyword in keywords if keyword in not_exist_keyword_set])
        if not_exists_count == 0:
            new_keywords_list.append(keywords)

    return new_keywords_list


def save_to_excel(datas: List[Dict]):
    pd.DataFrame(datas).to_excel("index.xlsx")


def get_search_index_demo(keywords_list: List[List[str]]):
    """
        1. 先清洗keywords数据，把没有收录的关键词拎出来
        2. 然后split_keywords关键词正常请求
        3. 数据存入excel
    """
    print("{} 开始清洗关键词".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    requested_keywords = []
    keywords_list = get_clear_keywords_list(keywords_list)
    q = Queue(-1)

    for splited_keywords_list in split_keywords(keywords_list):
        q.put(splited_keywords_list)
    
    print("{} 开始请求百度指数".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    datas = []
    while not q.empty():
        cur_keywords_list = q.get()
        try:
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 开始请求: {cur_keywords_list}")

            for i, area in enumerate(area_ids):
                print("{} area进度：{}/{}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),i+1, len(area_ids)))

                if "search" in index_type:
                    search_index_list = get_search_index(
                        keywords_list=cur_keywords_list,
                        start_date=start_date,
                        end_date=end_date,
                        cookies=cookies,
                        area=area
                    )
                    for index in search_index_list:
                        index["keyword"] = ",".join(index["keyword"])
                        index["type"] = 'search'
                        datas.append(index)
                if "feed" in index_type:
                    feed_index_list = get_feed_index(
                        keywords_list=cur_keywords_list,
                        start_date=start_date,
                        end_date=end_date,
                        cookies=cookies,
                        area=area
                    )
                    for index in feed_index_list:
                        index["keyword"] = ",".join(index["keyword"])
                        datas.append(index)
                requested_keywords.extend(cur_keywords_list)
                print(f"请求完成: {cur_keywords_list}")
                time.sleep(10)
        except:
            traceback.print_exc()
            print(f"请求出错, requested_keywords: {requested_keywords}")
            save_to_excel(datas)
            q.put(cur_keywords_list)
            time.sleep(180)

    save_to_excel(datas)


if __name__ == "__main__":
    keywords_list = [
        ["老赖"],["失信被执行人"],["失信人"],["违约"],["诈骗"],["欺诈"],["食言","爽约"]
    ]
    get_search_index_demo(keywords_list)
