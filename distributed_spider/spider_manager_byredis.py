#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import json
import hashlib
import multiprocessing
import time
import io
import os
import sys
reload(sys) 
sys.setdefaultencoding("utf-8")

import click
import redis


def echo(msg):
    print "\033[1;35m[TIP]:ID-{}, {}.\033[0m!".format(os.getpid(), msg)


class DataStorge(multiprocessing.Process):
    """数据管理保存, 实现功能：
    1. 将爬取信息添加存储器;
    2. 将爬取信息以本地文件形式输出,格式CSV.
    """
    def __init__(self, que, path='./', batch_num=10):
        multiprocessing.Process.__init__(self)
        self._redis = que
        self.batch_num = batch_num
        self._mv_downlinks = []
        self._interval = 3
        self.path = path
        echo("storage server is running")
        echo("output dir is " + path)

    def add_mvdownlink(self, data):
        """将爬取信息添加存储器"""
        print data
        self._mv_downlinks.append(data)
        if self.downlink_size() % self.batch_num == 0:
            echo("save url number is " + str(self.downlink_size())) 

    def downlink_size(self):
        return len(self._mv_downlinks)

    def output_data(self):
        """电影下载列表以csv格式输出"""
        fname = "movie_{}.csv".format(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
        with open(os.path.join(self.path, fname), 'w') as f:
            f.write('电影名称,电影链接\r\n')
            rows = map(lambda x: u"%s, %s" % (x[0], u",".join(x[-1])), self._mv_downlinks)
            f.write("\r\n".join(rows))
        echo(fname + " has saved")
        self._mv_downlinks = []

    def run(self):
        _rc = redis.StrictRedis(host=self._redis['HOST'], port=self._redis['PORT'],
                                    db=self._redis['DB'], password=self._redis['PW'])
        while 1:
            mvinfo = _rc.lpop(self._redis['QUEUES']['DATA_QUE'])
            if not mvinfo:
                time.sleep(self._interval)
                continue
            mvinfo = json.loads(mvinfo)
            self.add_mvdownlink((mvinfo.get('mv_name'), mvinfo.get('links')))
            if self.downlink_size() >= self.batch_num:
                self.output_data()


class URLManager(object):
    def __init__(self):
        self._urls = set()
        self._old_urls = set()

    def calcMD5(self, x):
        return hashlib.md5(x).hexdigest()

    def is_handled_url(self, url):
        return self.calcMD5(url) in self._old_urls

    def add_url(self, url):
        if not self.is_handled_url(url) and url not in self._urls:
            self._urls.add(url)

    def get_url(self):
        try:
            url = self._urls.pop()
            self._old_urls.add(self.calcMD5(url))
        except Exception:
            return None
        return url

    def muti_add_url(self, urls):
        for url in urls:
            self.add_url(url)
        return None

    def empty(self):
        return len(self._urls) == 0

    def rest_url_num(self):
        return len(self._urls)

    def handle_url_num(self):
        return len(self._old_urls)


def SpiderMain(base_url, que,
               max_page=200, page_index=1, interval=3, cache_num = 30):
    """实现爬虫调度器
    1. 初始化数据存储器、URL管理器、Redis队列
    2. 向URL管理器添加种子URL
    3. 从URL管理器获取URL，投放任务队列
    4. 从URL队列获得URL，纳入URL管理器统一管理
    """
    # 初始化URL管理器
    url_manager = URLManager()
    # 实例化数据存储器
    ds = DataStorge(que, './')
    ds.daemon = True
    ds.start()
    # 初始化redis连接对象
    rc = redis.StrictRedis(connection_pool=
                            redis.ConnectionPool(host=que['HOST'],
                                                port=que['PORT'],
                                                db=que['DB'],
                                                password=que['PW']))
    # 任务队列
    tque = que['QUEUES']['TASK_QUE']
    # 回馈url队列
    uque = que['QUEUES']['URL_QUE']
    # 添加种子url
    mvlist_path = "/html/gndy/dyzz/list_23_{0:d}.html"
    url_manager.add_url(base_url + mvlist_path.format(page_index))
    echo(str(page_index) + " url is " + base_url + mvlist_path.format(page_index))
    cache_num = max_page if cache_num > max_page else cache_num
    while 1:
        if rc.llen(tque) > cache_num:
            time.sleep(interval)
            continue
        # 从url管理器获取url，并投放到任务队列
        turls = [url_manager.get_url() for i in range(20)]
        [rc.rpush(tque, url) for url in turls if url]
        echo("handled url is {} and rest url is {}".format(url_manager.handle_url_num(),
                                                           url_manager.rest_url_num()))
        # 从url队列获取url，并加入url管理器
        urls = [rc.lpop(uque) for i in range(20)]
        [url_manager.add_url(url) for url in urls if url]
        if page_index < max_page:
            page_index += 1
            url_manager.add_url(base_url + mvlist_path.format(page_index))
            echo(str(page_index) + " url is " + base_url + mvlist_path.format(page_index))
        else:
            time.sleep(interval)


#
# 定义Redis数据队列：
# 1. 任务队列，投放待爬取url
# 2. 数据队列，爬取内容
# 3. url队列，爬取得到的URL
#
REDIS_CONF = {
    'HOST': 'localhost',
    'PORT': 6379,
    'DB': 0,
    'PW': '',
    'QUEUES': {
        'DATA_QUE': 'data_que',
        'TASK_QUE': 'task_que',
        'URL_QUE': 'url_que'
    }
}
if __name__ == "__main__":
    SpiderMain('http://www.dytt8.net', REDIS_CONF, max_page=4)
