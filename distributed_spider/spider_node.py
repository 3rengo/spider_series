#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Description: 分布式网络爬虫，爬虫节点
@Site: www.3sanrenxing.com
@Author: 青椒肉丝
'''
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from urlparse import urlparse
import time
import os

from bs4 import BeautifulSoup
import click
import requests

from spider_manager import echo


def extract_domain(url):
    """从url中提取域名"""
    res = urlparse(url)
    return res.scheme + "://" + res.netloc

def get_task(addr, api="/api/task"):
    """从调度管理节点获取待爬取任务"""
    r = requests.get("http://{}{}".format(addr, api))
    return r.text

def post_data(addr, data, api="/api/task/data"):
    """将爬取的数据和url信息上传调度管理节点"""
    r = requests.post("http://{}{}".format(addr, api), data=data)
    return r.status_code


class HTMLDownloader(object):
    """html内容下载器"""
    def __init__(self):
        self.head = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
                     "Referer": "http://www.dytt8.net/"
                     }

    def download(self, url):
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
        soup = BeautifulSoup(html, "html.parser", fromEncoding="gb18030")
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
        soup = BeautifulSoup(html, "html.parser", fromEncoding="gb18030")
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


@click.command()
@click.option("--addr", "-a", help="connect manager node server address, eg: ip:port")
def spider_node(addr):
    if not addr: raise Exception("addr isn't null")
    # 初始化html下载器
    hd = HTMLDownloader()
    # 初始化html解析器
    hp = HTMLParser()
    while 1:
        # 获取待爬取url
        url = get_task(addr)
        if url:
            spider_data = {"url": "", "mv_name": "", "links": ""} 
            domain = extract_domain(url)
            html = hd.download(url)
            if url.find("list_23_") >= 0:
                urls = hp.get_mvlist(html)
                spider_data['url'] = ",".join(map(lambda x: domain+x, urls))
                echo("spider urls is {}".format(spider_data['url']))
            else:
                spider_data['mv_name'], spider_data['links'] = hp.get_mv_downlink(url, html)
                echo("spider {mv_name} {links}".format(**spider_data))
            # 上传爬取数据
            post_data(addr, spider_data)
        else:
            time.sleep(2)

if __name__ == "__main__":
    spider_node()
