"""
百度指数数据获取最佳实践
此脚本完成
1. 清洗关键词
2. 发更少请求获取更多的数据
3. 请求容错
4. 容错后并保留当前已经请求过的数据，并print已请求过的keywords
"""
import random
import json
from queue import Queue
from typing import Dict, List
import datetime
import traceback
import time

import pandas as pd
from qdata.baidu_index import get_search_index
from qdata.baidu_index import get_feed_index
from qdata.baidu_index.common import check_keywords_exists, split_keywords
import mysql.mysql
cookies = []
# cookies.append("BAIDUID=C94E71F42A6F790D29D970728B032CE8:FG=1; BAIDUID_BFESS=C94E71F42A6F790D29D970728B032CE8:FG=1; BDUSS=FPN3V1dnY3bjFTWi13cmlCclFYUnY1MzF2UXp-LWJsWEdKZlc5YjdzTnVPeEptRVFBQUFBJCQAAAAAAAAAAAEAAABQWgVBYmVsaWV2ZTg5NAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG6u6mVuruplVT; BDUSS_BFESS=FPN3V1dnY3bjFTWi13cmlCclFYUnY1MzF2UXp-LWJsWEdKZlc5YjdzTnVPeEptRVFBQUFBJCQAAAAAAAAAAAEAAABQWgVBYmVsaWV2ZTg5NAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG6u6mVuruplVT; BIDUPSID=C94E71F42A6F790D29D970728B032CE8; H_PS_PSSID=39662_40207_40212_40217_40294_40291_40287_40285_40079_40364_40352_40373_40367_40317; PSTM=1709878894; bdindexid=baro17vijkbfemvour9sjf5li2; PTOKEN=c74f1608e72a402842aadb7f9cfbc521; PTOKEN_BFESS=c74f1608e72a402842aadb7f9cfbc521; STOKEN=6ede54cc33e9ae5ae4ed32c1019f87a7b731beec6238a69fd3dd37e52904b2a7; STOKEN_BFESS=6ede54cc33e9ae5ae4ed32c1019f87a7b731beec6238a69fd3dd37e52904b2a7; UBI=fi_PncwhpxZ%7ETaJcxFmvzU7ni8BBW0NmP2Y; UBI_BFESS=fi_PncwhpxZ%7ETaJcxFmvzU7ni8BBW0NmP2Y; __yjs_st=2_YWNjY2IyNzk5MTY5MWExNTU4NmQ1MTczNTM3YmQwZjUzOGJlODkxMjc5ZmQ0YmU2NDg4ZmI1ZGMxMjY2OTNlYzRjMTQ5ZGFhOTBkMzBlNTgzZGJiNTczZTg4NDk4YzM5NTFkYjNhZmRhNGViYmZjNzc0YjNiMDM4MTgxYjM4NTAyMDE1ZTgxOTE1N2E0NDliODI5ZWQ0NGE4YWY2YmMzYTM3MzI2N2Y0YmRiNjc0YTk5NDYzMWJkMzI5M2RiMGI2XzdfYTk3NjRjYzc=")
# cookies.append("BAIDUID=35242943A7EC7E3C01B479A25475FD81:FG=1; BAIDUID_BFESS=35242943A7EC7E3C01B479A25475FD81:FG=1; BDUSS=NqcllJbHB6YkVCQzItaGdifmRQOFR-c29KWG1Yczd6Z084V2NOcG1HMlpCUlJtRVFBQUFBJCQAAAAAAQAAAAEAAADBIa4sAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJl47GWZeOxlcW; BDUSS_BFESS=NqcllJbHB6YkVCQzItaGdifmRQOFR-c29KWG1Yczd6Z084V2NOcG1HMlpCUlJtRVFBQUFBJCQAAAAAAQAAAAEAAADBIa4sAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJl47GWZeOxlcW; BIDUPSID=35242943A7EC7E3C01B479A25475FD81; PSTM=1709996185; bdindexid=liltg0ji55lojjv6lov32q25t3; PTOKEN=3016de186f30d1b99d86053af6605275; PTOKEN_BFESS=3016de186f30d1b99d86053af6605275; STOKEN=2cd445813d9d6b08620a3ebaeca9e71eb0fd905000b03536361a3eb84bc5e6bb; STOKEN_BFESS=2cd445813d9d6b08620a3ebaeca9e71eb0fd905000b03536361a3eb84bc5e6bb; UBI=fi_PncwhpxZ%7ETaJc-ZQr4XdgbyU0s9x7vkY; UBI_BFESS=fi_PncwhpxZ%7ETaJc-ZQr4XdgbyU0s9x7vkY; __yjs_st=2_NjY1MDk3YmQ1Y2U4NDU1YjI5YzQ1MWRkNTYzYTU3MjhmNGRjMjA2ZDdiNGJiYWU4NGQwMmRlMGU5MTZlMzk4ODEwYzk2N2MzYmQxOTA5NWU1MjNmOWE1YzM5YzljYTJiMDAzNWMxNTcyOTA4YzczNmYyMzFjN2VlNGZiNjU4MGRiNWI4Y2EzNzI4YjgwMTFiN2M4NjdhMGMzNDE3YTY5ZTA2MDJjYjFlZWFmZTczNDA3ZDU5OGVjYTQzZjVmZTk3NzcwNWM0NzkyZTc5OGZlMDM3OWI5ODc4NjY5NmFmNGY5ZTM3MmFkY2M5NDA0ODI2NzU0MDFjODkzNGM1YjYwOF83X2UxYmQ4MDcx")
# cookies.append("BAIDUID=96E366727FD9EBF2F4A89C040BE258D9:FG=1; BAIDUID_BFESS=96E366727FD9EBF2F4A89C040BE258D9:FG=1; BDUSS=U5FeWNmWkJTSXNVTUVreDA0MTVrMmE4cUtsVjM2cTQzQjZPUkZqWW5yZzV0Uk5tRVFBQUFBJCQAAAAAAAAAAAEAAAD56I9hc3VubnlCYWJ5ZmFjZTIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADko7GU5KOxlW; BDUSS_BFESS=U5FeWNmWkJTSXNVTUVreDA0MTVrMmE4cUtsVjM2cTQzQjZPUkZqWW5yZzV0Uk5tRVFBQUFBJCQAAAAAAAAAAAEAAAD56I9hc3VubnlCYWJ5ZmFjZTIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADko7GU5KOxlW; BIDUPSID=96E366727FD9EBF2F4A89C040BE258D9; PSTM=1709975609; bdindexid=7oqgul4pi6blkjue1iuat64rd4; PTOKEN=b1ba14a8abe727e954621ca914294558; PTOKEN_BFESS=b1ba14a8abe727e954621ca914294558; STOKEN=726b2abefbb42bd5a37b5e7cb8c15696404340b89c86f6d63e18381aaed59f12; STOKEN_BFESS=726b2abefbb42bd5a37b5e7cb8c15696404340b89c86f6d63e18381aaed59f12; UBI=fi_PncwhpxZ%7ETaJc0ZWzMxP4Kdy0UxDbrUn; UBI_BFESS=fi_PncwhpxZ%7ETaJc0ZWzMxP4Kdy0UxDbrUn; __yjs_st=2_NjY1MDk3YmQ1Y2U4NDU1YjI5YzQ1MWRkNTYzYTU3MjhmNGRjMjA2ZDdiNGJiYWU4NGQwMmRlMGU5MTZlMzk4ODEwYzk2N2MzYmQxOTA5NWU1MjNmOWE1YzM5YzljYTJiMDAzNWMxNTcyOTA4YzczNmYyMzFjN2VlNGZiNjU4MGRiNWI4Y2EzNzI4YjgwMTFiN2M4NjdhMGMzNDE3YTY5ZTQ4NGI5NDMwMDEwMmVlZGUyZjgwYmRkZTlhNGZjYmIzOTdjYTIzYzAwMTE5NjAyMzM2YmEzYmQ3MjEzOGYyMGU1NWNmYmJmMjIwZTU3YTZmYTdjODRmYjM2MjIzZGU1Nl83X2E5ZGQ3MzAx")
cookies.append("BAIDUID=D090FCAEA9C0F1C56EC672F1D24752BE:FG=1; BAIDUID_BFESS=D090FCAEA9C0F1C56EC672F1D24752BE:FG=1; BDUSS=hCRFJGdWVJQ3d-czVtSmdFcVB5S0lRTFRZcjBJbHBZUy1oRURwQmotWkRTQnBtRVFBQUFBJCQAAAAAAAAAAAEAAADnetuRMTEyeWl3ZWkAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEO78mVDu~Jlck; BDUSS_BFESS=hCRFJGdWVJQ3d-czVtSmdFcVB5S0lRTFRZcjBJbHBZUy1oRURwQmotWkRTQnBtRVFBQUFBJCQAAAAAAAAAAAEAAADnetuRMTEyeWl3ZWkAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEO78mVDu~Jlck; BIDUPSID=D090FCAEA9C0F1C56EC672F1D24752BE; H_PS_PSSID=39662_40210_40207_40216_40244_40294_40291_40287_40286_40080_40365_40351_40300_40373_40317_40337_40415; PSTM=1710406467; bdindexid=7iodkouodhsqsce684cf7pq094; PTOKEN=0013286f8ed5b33a2efa191c9efa3048; PTOKEN_BFESS=0013286f8ed5b33a2efa191c9efa3048; STOKEN=bc7279c20b46a71ce38ae0427946cd1d14d5b5d8a94c9af4e9f3b5b697307018; STOKEN_BFESS=bc7279c20b46a71ce38ae0427946cd1d14d5b5d8a94c9af4e9f3b5b697307018; UBI=fi_PncwhpxZ%7ETaJczzpV4cES%7ElXLZ4AugNG; UBI_BFESS=fi_PncwhpxZ%7ETaJczzpV4cES%7ElXLZ4AugNG; __yjs_st=2_YWNjY2IyNzk5MTY5MWExNTU4NmQ1MTczNTM3YmQwZjUzOGJlODkxMjc5ZmQ0YmU2NDg4ZmI1ZGMxMjY2OTNlYzRjMTQ5ZGFhOTBkMzBlNTgzZGJiNTczZTg4NDk4YzM5NTFkYjNhZmRhNGViYmZjNzc0YjNiMDM4MTgxYjM4NTA0MmUxYjY3YmNjNjZiMDU4MjdhNjJlOWRhMDk1MzJmYmEzMTU0ODkwYzc5YjY1YjFiZmI1MTQ4MWJmZDdlZmQ3XzdfYTg5YjdmMGU=")
excel_name_prefix="搜索指数avg-"
index_type = []
index_type.append("search")
index_type.append("feed")
# area_ids=[0]
area_ids = [0, 901, 902, 903, 904, 905, 906, 907, 908, 909, 910, 911, 912, 913, 914, 915, 916, 917, 918, 919, 920, 921,922, 923, 924, 925, 926, 927, 928, 929, 930, 931, 932, 933, 934, 95, 94, 133, 195, 196, 197, 198, 199, 200,201, 202, 203, 204, 205, 207, 208, 209, 210, 211, 212, 213, 168, 262, 263, 264, 265, 266, 268, 370, 371,373, 374, 375, 376, 378, 379, 380, 381, 667, 97, 96, 98, 99, 100, 101, 102, 103, 104, 106, 107, 108, 109,111, 112, 113, 114, 291, 417, 457, 479, 125, 126, 127, 156, 157, 158, 159, 160, 161, 162, 163, 169, 172, 28,30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 73, 74, 687, 138, 134, 135, 149, 287, 288, 289, 303,304, 305, 306, 50, 51, 52, 53, 54, 55, 56, 87, 253, 152, 153, 295, 297, 300, 301, 302, 319, 320, 322, 323,324, 359, 1, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 88, 352, 353, 356, 366, 165, 271, 272, 273, 274,275, 276, 277, 278, 401, 141, 143, 144, 145, 146, 147, 148, 259, 261, 292, 293, 150, 29, 151, 215, 216, 217,218, 219, 220, 221, 222, 223, 224, 225, 154, 155, 191, 194, 270, 407, 408, 410, 525, 117, 123, 124, 334,335, 337, 339, 342, 350, 437, 438, 666, 668, 669, 671, 672, 467, 280, 310, 311, 312, 315, 317, 318, 383,384, 386, 499, 520, 563, 653, 661, 692, 693, 90, 89, 91, 92, 93, 118, 119, 128, 129, 130, 131, 132, 506,665, 231, 227, 228, 229, 230, 232, 233, 234, 235, 236, 237, 43, 44, 45, 46, 47, 48, 49, 65, 66, 67, 68, 226,269, 405, 5, 6, 7, 8, 9, 10, 115, 136, 137, 246, 256, 189, 173, 174, 175, 176, 177, 178, 179, 181, 182, 183,184, 185, 186, 187, 188, 391, 20, 13, 14, 15, 16, 17, 19, 21, 22, 25, 331, 333, 166, 281, 282, 283, 284,285, 286, 307, 308, 309, 343, 344, 346, 673, 239, 241, 242, 243, 244, 456, 582, 670, 674, 675, 679, 680,681, 683, 684, 686, 689, 690, 2, 3, 4, 59, 61, 422, 424, 426, 588, 140, 395, 396, 472, 480, 139, 608, 652,659, 676, 682, 685, 688, 466, 516, 655, 656, 677, 678, 691]
start_date = "2020-01-01"
end_date = "2021-01-01"
mysql_client = mysql.mysql.MySQLClient()
area_map = {item["area_id"]:item for item in json.load(open("../area_code_map.json"))}

def get_clear_keywords_list(keywords_list: List[List[str]]) -> List[List[str]]:
    q = Queue(-1)

    cur_keywords_list = []
    for keywords in keywords_list:
        cur_keywords_list.extend(keywords)

    # 先找到所有未收录的关键词
    for start in range(0, len(cur_keywords_list), 15):
        q.put(cur_keywords_list[start:start + 15])

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


def save_to_excel(search: List[Dict],feed: List[Dict], name):
    rsuf="_r"
    if len(search)>0 and len(feed)>0:
        f = pd.DataFrame(search).join(pd.DataFrame(feed), how='left', rsuffix=rsuf)
        drop_c = [s+rsuf for s in ["keyword","date","area","area_name","parent_area_id","area_level"]]
        f=f.drop(drop_c, axis=1)
    elif len(search)>0 and len(feed)==0:
        f=pd.DataFrame(search)
    elif len(feed)>0 and len(search)==0:
        f = pd.DataFrame(feed)
    else:
        return
    f.to_excel(f"{name}.xlsx")


def get_index_demo(keywords_list: List[List[str]]):
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
        for id in area_ids:
            q.put((splited_keywords_list,id))

    print("{} 开始请求百度指数".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    i=0
    e=0
    while not q.empty():
        cur_keywords_list, cur_area_id = q.get()
        cookie = random.choice(cookies)
        try:
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 开始请求: {cur_keywords_list},{cur_area_id},成功：{i} 剩余：{q.qsize()}")

            if "search" in index_type:
                search_index_list = get_search_index(
                    keywords_list=cur_keywords_list,
                    start_date=start_date,
                    end_date=end_date,
                    cookies=cookie,
                    area=cur_area_id
                )
                for index in search_index_list:
                    index["keyword"] = ",".join(index["keyword"])
                    index["type"] = 'search'
                    update_area_info(index, cur_area_id)
                    mysql_client.upsert_index(index)
                time.sleep(10)
            if "feed" in index_type:
                feed_index_list = get_feed_index(
                    keywords_list=cur_keywords_list,
                    start_date=start_date,
                    end_date=end_date,
                    cookies=cookie,
                    area=cur_area_id
                )
                for index in feed_index_list:
                    index["keyword"] = ",".join(index["keyword"])
                    update_area_info(index, cur_area_id)
                    mysql_client.upsert_index(index)
                time.sleep(10)
            print(f"请求完成: {cur_keywords_list}")
            i = i+1
            e=0
        except:
            traceback.print_exc()
            print(f"请求出错, requested_keywords: {cur_keywords_list} area_id:{cur_area_id} cookie:{cookie}")
            q.put((cur_keywords_list, cur_area_id))
            time.sleep(60)
            e = e+1
            if e>2:
                break

def update_area_info(index,cur_area_id):
    index["area_name"] = area_map[cur_area_id]["area_name"]
    index["area_level"] = area_map[cur_area_id]["level"]
    index["parent_area_id"] = area_map[cur_area_id]["parent_id"]
def get_avg_index(keywords_list):
    q = Queue(-1)
    for splited_keywords_list in split_keywords(keywords_list):
        for id in area_ids:
            q.put((splited_keywords_list, id))

    print("{} 开始请求百度指数avg".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    datas = []
    feed_data = []
    search_data = []
    i = 0
    e=0
    while not q.empty():
        cur_keywords_list, cur_area_id = q.get()
        cookie = random.choice(cookies)
        try:
            print(
                f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 开始请求: {cur_keywords_list},{cur_area_id},成功：{i} 剩余：{q.qsize()}")

            if "search" in index_type:
                search_index_list = get_search_index(
                    keywords_list=cur_keywords_list,
                    start_date=start_date,
                    end_date=end_date,
                    cookies=cookie,
                    area=cur_area_id,
                    split_time=False
                )
                pre_keyword = ''
                for index in search_index_list:
                    index["keyword"] = ",".join(index["keyword"])
                    cur_keyword = index["keyword"]
                    if pre_keyword==cur_keyword:
                        continue
                    index["type"] = 'search'
                    index["area_level"] = 'search'
                    index.pop("index")
                    update_area_info(index, cur_area_id)
                    search_data.append(index)
                    pre_keyword=cur_keyword
            if "feed" in index_type:
                feed_index_list = get_feed_index(
                    keywords_list=cur_keywords_list,
                    start_date=start_date,
                    end_date=end_date,
                    cookies=cookie,
                    area=cur_area_id,
                    split_time=False
                )
                for index in feed_index_list:
                    index["keyword"] = ",".join(index["keyword"])
                    cur_keyword = index["keyword"]
                    if pre_keyword == cur_keyword:
                        continue
                    update_area_info(index, cur_area_id)
                    index.pop("index")
                    feed_data.append(index)
                    pre_keyword = cur_keyword
            time.sleep(1)
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 请求完成: {cur_keywords_list}")
            i = i + 1
            e=0
        except:
            traceback.print_exc()
            # save_to_excel(search_data,feed_data, start_date)
            print(f"请求出错, requested_keywords: {cur_keywords_list} area_id:{cur_area_id} cookie:{cookie}")
            q.put((cur_keywords_list, cur_area_id))
            e = e + 1
            if e > 2:
                break

    save_to_excel(search_data, feed_data, excel_name_prefix+str(start_date))


if __name__ == "__main__":
    keywords_list = [
        ["老赖"],["失信被执行人"],["失信人"],["违约"],["诈骗"],["欺诈"],["食言","爽约"]
    ]
    ## 咨询指数+搜索指数
    # get_index_demo(keywords_list)
    ## 平均值计算
    get_avg_index(keywords_list)

