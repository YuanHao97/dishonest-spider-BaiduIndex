#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2024/3/9 20:55
# @Author     : Hao Yuan
import pymysql

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "dishonest"
MYSQL_USER = "root"
MYSQL_PASSWORD = "rootroot"


# create table dishonesty(dt int, area_id int, area_name varchar(32), keywords varchar(128), search_index bigint, feed_index bigint, UNIQUE KEY `k` (`dt`,`area_id`,`keywords`));

class DishonestEntity:
    dt: str
    area_id: int
    area_name: str
    keywords: str
    search_index: int
    feed_index: int

    def to_dict(self):
        d={}
        if hasattr(self, "dt"):
            d["dt"] = self.dt
        if hasattr(self, "area_id"):
            d["area_id"] = self.area_id
        if hasattr(self, "area_name"):
            d["area_name"] = self.area_name
        if hasattr(self, "keywords"):
            d["keywords"] = self.keywords
        if hasattr(self, "search_index"):
            d["search_index"] = self.search_index
        if hasattr(self, "feed_index"):
            d["feed_index"] = self.feed_index
        return d


def from_row(row: list, d: DishonestEntity) -> DishonestEntity:
    if row[0] is not None:
        d.dt = row[0]
    if row[1] is not None:
        d.area_id = row[1]
    if row[2] is not None:
        d.area_name = row[2]
    if row[3] is not None:
        d.keywords = row[3]
    if row[4] is not None:
        d.search_index = row[4]
    if row[5] is not None:
        d.feed_index = row[5]
    return d


class MySQLClient:

    def __init__(self):
        # 建立数据库连接
        self.connection = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, db=MYSQL_DB,
                                          user=MYSQL_USER, password=MYSQL_PASSWORD)
        # 获取操作数据的cursor
        self.cursor = self.connection.cursor()

    def __del__(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def upsert_index(self, d: dict):
        entity = DishonestEntity()
        entity.keywords = d["keyword"]
        entity.dt = d["date"].replace("-","")
        entity.area_id = d["area"]
        entity.area_name = d["area_name"]
        if d["type"] == "feed":
            entity.feed_index = d["index"]
        elif d["type"] == "search":
            entity.search_index = d["index"]
        else:
            return
        self.upsert(entity)

    def upsert(self, dishonesty: DishonestEntity):
        self.cursor.execute(
            "select * from dishonesty where area_id={} and dt={} and keywords='{}'"
            .format(dishonesty.area_id, dishonesty.dt, dishonesty.keywords))
        row = self.cursor.fetchone()
        if row is None:
            keys, values = zip(*dict(dishonesty.to_dict()).items())
            insert_sql = 'INSERT INTO dishonesty ({}) VALUES ({})'.format(
                ','.join(keys),
                ','.join(['%s'] * len(values))
            )
            # 执行SQL
            self.cursor.execute(insert_sql, values)
            # 提交事务
            self.connection.commit()
            # print('插入数据,{}'.format(dishonesty.to_dict()))
        else:
            newRow = from_row(row, dishonesty)
            update_sql = 'update dishonesty set {} where dt={} and area_id={} and keywords="{}"'.format(
                ','.join(["{}='{}'".format(k, v) for k, v in newRow.to_dict().items()]),
                newRow.dt,
                newRow.area_id,
                newRow.keywords
            )
            # 执行SQL
            self.cursor.execute(update_sql)
            # 提交事务
            self.connection.commit()
            # print('更新数据 {}'.format(newRow.to_dict()))


if __name__ == "__main__":
    client = MySQLClient()
    d = DishonestEntity()
    l = [2000, 2, "全国", "ll", 124, 123]
    client.upsert(from_row(l, d))
