#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Description: Python版 基础爬虫示例
@Site: www.3sanrenxing.com
@Author: 青椒肉丝
@Date: 2018-10-04 13:24:38
@LastEditTime: 2018-11-03 13:34:48
'''
import sys 
reload(sys) 
sys.setdefaultencoding("utf-8")
import csv
import urllib2
import time
import os

from bs4 import BeautifulSoup


class URLManager(object):
    """URL管理器，实现功能包括：
    1. 添加URL、批量添加URL
    2. 获取待爬取URL
    3. 是否有待爬取URL
    4. 待爬取URL数量
    5. 以爬取URL数量
    """
    def __init__(self):
        self._urls = set()
        self._old_urls = set()
    
    def add_url(self, url):
        if url not in self._old_urls and url not in self._urls:
            self._urls.add(url)
    
    def get_url(self):
        url = self._urls.pop()
        self._old_urls.add(url)
        return url
    
    def muti_add_url(self, urls):
        for url in urls:
            self.add_url(url)
        return None

    def has_url(self):
        return len(self._urls) != 0

    def rest_url_num(self):
        return len(self._urls)

    def handle_url_num(self):
        return len(self._old_urls)


class HTMLDownloader(object):
    """html内容下载器"""
    def __init__(self):
        self.head = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
                     "Referer": "http://www.dytt8.net/"
                     }
                    
    def download(self, url, data=None):
        try:
            request = urllib2.Request(url, data, self.head)
            resp = urllib2.urlopen(request).read()
        except Exception:
            return ""
        else:
            return resp


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
            items = soup.find(class_='co_content8').find('ul').find_all('table')
            items = filter(lambda x: bool(x), items)
        except Exception:
            # 未发现电影列表
            return []
        def parse(tag):
            a_tag = tag.find('a', href=True, class_='ulink')
            return TARGET_SITE + a_tag.attrs.get("href")
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


class DataStorge(object):
    """有效数据管理保存, 实现功能：
    1. 添加保存有效数据;
    2. 将保存的有效数据，以本地文件形式输出,格式CSV.
    """
    def __init__(self):
        self._header = ['电影名称', '电影链接']
        self._mv_downlinks = []
    
    def save_mvdownlink(self, data):
        self._mv_downlinks.append(data)

    def output_data(self, path):
        with open(path, 'w') as f:
            fcsv = csv.writer(f)
            fcsv.writerow(self._header)
            fcsv.writerows(self._mv_downlinks)
        self._mv_downlinks = {}


class SpiderMain(object):
    """爬虫调度器"""
    def __init__(self):
        # 初始化
        self.url_manager = URLManager()
        self.downloader = HTMLDownloader()
        self.parser = HTMLParser()
        self.data_op = DataStorge()
        self.mvlist_path = "/html/gndy/dyzz/list_23_{0:d}.html"
        self.page_index = 1
    
    def crawl(self, base_url):
        # 添加第一个url
        self.url_manager.add_url(base_url+self.mvlist_path.format(self.page_index))
        # self.url_manager.handle_url_num() <= 1000, 限制爬取量
        while self.url_manager.has_url() and self.url_manager.handle_url_num() <= 1000:
            url = self.url_manager.get_url()
            path_name = os.path.basename(url)
            # 控制爬取速度，过于频繁，容易被禁
            time.sleep(0.5)
            # 解析两种类型url，一个是电影列表页，一个是电影详情页
            # 电影列表页有电影详情页地址链接
            # 电影详情页有电影的下载链接
            if path_name.startswith("list_23_"):
                '''处理电影列表页'''
                html = self.downloader.download(url)
                if not html: continue
                mvs = self.parser.get_mvlist(html)
                if mvs:
                    # 向url管理器添加URL
                    self.url_manager.muti_add_url(mvs)
                    self.page_index += 1
                    self.url_manager.add_url(base_url+self.mvlist_path.format(self.page_index))
                    print "进度-A: 正在获取第{0:d}电影列表页".format(self.page_index)
            else:
                '''解析获取电影下载链接'''
                html = self.downloader.download(url)
                if not html: continue
                mv_name, links = self.parser.get_mv_downlink(url, html)
                # 保存电影下载链接
                self.data_op.save_mvdownlink((mv_name, ",".join(links)))
            print "进度-B: 待处理URL数: {0:d},以处理URL: {1:d}".format(self.url_manager.rest_url_num(),
                                                                    self.url_manager.handle_url_num())
        # 电影的下载链接，以CSV格式保存本地
        self.data_op.output_data('./movies.csv')


if __name__ == '__main__':
    TARGET_SITE = 'http://www.dytt8.net'
    crawler = SpiderMain()
    crawler.crawl(TARGET_SITE)
