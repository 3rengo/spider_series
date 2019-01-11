#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sys
from urlparse import urlparse
import time
import os
import sys
reload(sys) 
sys.setdefaultencoding("utf-8")

from bs4 import BeautifulSoup
import click
import requests
import redis
from spider_manager_byredis import echo


def extract_domain(url):
    """从url中提取域名"""
    res = urlparse(url)
    return res.scheme + "://" + res.netloc


class HTMLDownloader(object):
    """html内容下载器"""

    def __init__(self):
        self.head = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
                     "Referer": "http://www.dytt8.net/"
                     }

    def download(self, url, delay=0):
        time.sleep(delay)
        r = requests.get(url, headers=self.head)
        return r.content


class HTMLParser(object):
    """html解析器,实现功能：
    1. 解析电影列表页，获取电影详情页地址
    2. 解析电影详情页, 获取电影下载地址
    """
    def __init__(self):
        pass

    def get_mvlist(self, html):
        """解析html，获取电影详情地址"""
        soup = BeautifulSoup(html, "html.parser")
        try:
            items = soup.find(class_='co_content8').find(
                'ul').find_all('table')
            items = filter(lambda x: bool(x), items)
        except Exception:
            # 未发现电影列表
            return []

        def parse(tag):
            a_tag = tag.find('a', href=True, class_='ulink')
            return a_tag.attrs.get("href")
        return map(parse, items)

    def get_mv_downlink(self, addr, html):
        """解析html，获取电影下载链接"""
        soup = BeautifulSoup(html, "html.parser")
        kw_tag = soup.find('meta', attrs={"name": 'keywords'})
        mv_name = kw_tag['content']
        mv_name = mv_name[:mv_name.rfind(u"下载")]
        links = []
        for tag in soup.find_all('table', align='center'):
            atag = tag.find('a')
            try:
                dlink = atag.get_text()
                if len(dlink) < 10:
                    print 'ERR[+]: ', addr
                else:
                    links.append(dlink)
            except Exception:
                print 'ERR[+]: ', addr
        return mv_name, links


def spider_node(que, interval=3):
    """实现爬虫节点
    1. 初始化HTML下载器、HTML解析器、redis连接对象等
    2. 从任务队列获取URL，解析URL对应HTML内容，得到新的URL和电影信息
    3. 将解析得到URL增加URL队列
    4. 将解析到电影信息添加到数据队列
    """
    # 初始化html下载器
    hd = HTMLDownloader()
    # 初始化html解析器
    hp = HTMLParser()
    # 初始化redis连接对象
    rc = redis.StrictRedis(host=que['HOST'], port=que['PORT'],
                            db=que['DB'], password=que['PW'])
    # 任务队列
    tque = que['QUEUES']['TASK_QUE']
    # 回馈url队列
    uque = que['QUEUES']['URL_QUE']
    # 数据队列
    dque = que['QUEUES']['DATA_QUE']
    while 1:
        # 获取待爬取url
        url = rc.lpop(tque)
        if url:
            domain = extract_domain(url)
            html = hd.download(url, 1)
            if url.find("list_23_") >= 0:
                urls = hp.get_mvlist(html)
                urls = map(lambda x: domain + x, urls)
                # 上传URL
                [rc.rpush(uque, url) for url in urls]
                echo("spider urls is {}".format(",".join(urls)))
            else:
                spider_data = {}
                spider_data['mv_name'], spider_data['links'] = hp.get_mv_downlink(url, html)
                echo("spider {mv_name} {links}".format(**spider_data))
                # 上传电影信息
                rc.rpush(dque, json.dumps(spider_data))
        else:
            time.sleep(interval)


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
    spider_node(REDIS_CONF)
