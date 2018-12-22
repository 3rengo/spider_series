#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Description: 分布式网络爬虫，爬虫调度管理端
@Site: www.3sanrenxing.com
@Author: 青椒肉丝
'''
from BaseHTTPServer import HTTPServer
import csv
import datetime
import json
import hashlib
import multiprocessing
import time
import io
import os
import urllib
from SimpleHTTPServer import SimpleHTTPRequestHandler
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import click


def echo(msg):
    print "\033[1;35m[TIP]:ID-{}, {}.\033[0m!".format(os.getpid(), msg)


class DataStorge(multiprocessing.Process):
    """数据管理保存, 实现功能：
    1. 将爬取信息添加存储器;
    2. 将爬取信息以本地文件形式输出,格式CSV.
    """
    def __init__(self, rqueue,
                    table_header=['movie_name', 'downlink'],
                    path='./'):
        multiprocessing.Process.__init__(self)
        self._header = table_header
        self._mv_downlinks = []
        self._rqueue = rqueue
        self._interval = 3
        self.path = path
        echo("storage server is running")
        echo("output dir is " + path)
        
    def add_mvdownlink(self, data):
        """将爬取信息添加存储器"""
        self._mv_downlinks.append(data)
        if self.downlink_size() % 10 == 0:
            echo("save url number is " + str(self.downlink_size())) 

    def downlink_size(self):
        return len(self._mv_downlinks)

    def output_data(self):
        """电影下载列表以csv格式输出"""
        fname = "movie_{}.csv".format(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
        with open(os.path.join(self.path, fname), 'w') as f:
            fcsv = csv.writer(f)
            fcsv.writerow(self._header)
            fcsv.writerows(map(lambda x: (x.get("mv_name"), x.get("links")), self._mv_downlinks))
        echo(fname + " has saved")
        self._mv_downlinks = []

    def run(self):
        while 1:
            if self.downlink_size() >= 100:
                self.output_data()
            if not self._rqueue.empty():
                mvinfo = self._rqueue.get()
                self.add_mvdownlink(mvinfo)
            else:
                time.sleep(self._interval)


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
        url = self._urls.pop()
        self._old_urls.add(self.calcMD5(url))
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


class SpiderMain(SimpleHTTPRequestHandler):
    """创建HTTP服务，实现爬虫调度管理端与其他爬虫节点通过网络进行通信
    1. 通过该服务，爬虫节点可获取带爬取URL
    2. 通过该服务，爬虫节点可将爬取的数据传输到调度管理，进行数据存储
    """
    protocol_version = "HTTP/1.1"
    server_version = "PSHS/0.1s"
    ERROR_MSG = {'code': -1, 'info': 'not found'}
    OK_MSG = {'code': 0, 'info': 'OK'}
    rqueue = None
    tqueue = None
    uqueue = None

    def get_task(self):
        """从任务队列获取任务"""
        try:
            url = SpiderMain.tqueue.get(timeout=1)
        except Exception:
            url = ""
        return url

    def do_GET(self):
        """爬虫节点通过该接口获取待爬取url
           GET  /api/task 
        """
        if self.path == '/api/task':
            url = self.get_task()
            echo("assign " + url)
            self.send(url)
        else:
            self.send(json.dumps(SpiderMain.ERROR_MSG), code=404)

    def do_POST(self):
        """爬虫节点通过该接口上传数据和新url
           POST  /api/task/data
           {'url': [], 'result': {}}
        """
        if self.path == "/api/task/data":
            data = self.rfile.read(int(self.headers["content-length"]))
            data = urllib.unquote(data)
            data = self.parse_data(data)
            if data.get('url', ""):
                # 更新url队列
                for url in data.get('url', "").split(","):
                    SpiderMain.uqueue.put(url)
            if data.get('mv_name', ""):
                # 更新结果队列
                data.pop("url")
                SpiderMain.rqueue.put(data)
            self.send(json.dumps(SpiderMain.OK_MSG))
        else:
            self.send(json.dumps(SpiderMain.ERROR_MSG))

    def parse_data(self, data):
        ranges = {}
        for item in data.split("&"):
            k, v = item.split("=")
            ranges[k] = v
        return ranges

    def send(self, content, code=200, type="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        f = io.BytesIO()
        f.write(content)
        f.seek(0)
        self.copyfile(f, self.wfile)
        f.close()


def url_update(base_url, tqueue, uqueue,  page_index=1):
    """调度管理器下，负责URL同步
    1. 从url队列获取url，并添加到url管理器；
    2. 从url管理器获取待爬取url，并添加到任务队列
    """
    # 初始化
    url_manager = URLManager()
    mvlist_path = "/html/gndy/dyzz/list_23_{0:d}.html"
    # 添加种子url
    echo(str(page_index) + " url is " + base_url + mvlist_path.format(page_index))
    url_manager.add_url(base_url + mvlist_path.format(page_index))
    url = url_manager.get_url()
    tqueue.put(url)
    while 1:
        echo("handled url is {} and rest url is {}".format(url_manager.handle_url_num(),
                                                        url_manager.rest_url_num()))
        while not uqueue.empty():
            url = uqueue.get()
            url_manager.add_url(url)
        while tqueue.empty() and url_manager.rest_url_num() != 0:
            tqueue.put(url_manager.get_url())
        if url_manager.rest_url_num() == 0:
            page_index += 1
            echo(str(page_index) + " url is " + base_url + mvlist_path.format(page_index))
            url_manager.add_url(base_url + mvlist_path.format(page_index))
        time.sleep(10)

#
# 初始化通讯队列：
# 1. 任务队列，投放待爬取url
# 2. 结果队列，存放爬取信息和新获取到的url
# 3. url队列，建立url管理器与调度管理器通讯
#
task_queue = multiprocessing.Queue()
result_queue = multiprocessing.Queue()
url_queue = multiprocessing.Queue()

@click.command()
# 指定端口
@click.option("--port", "-p", type=int, default=8001, help="start spider manager server, set port!")
def crawler(port):
    ds = DataStorge(result_queue, [u'电影名称', u'下载链接'], './')
    ds.daemon = True
    ds.start()
    SpiderMain.uqueue = url_queue
    SpiderMain.tqueue = task_queue
    SpiderMain.rqueue = result_queue
    # url同步操作，与URL管理器通讯，获取待爬取url，更新URL管理器中的url
    updater = multiprocessing.Process(target=url_update,
                                        args=('http://www.dytt8.net', task_queue, url_queue, 1,))
    updater.daemon = True
    updater.start()
    try:
        # 启动http服务，爬虫任务节点通过该服务接口，获取爬虫任务，并将爬取结果上传至调度管理端
        echo("server 0.0.0.0:{} is running".format(port))
        server = HTTPServer(("", port), SpiderMain)
        server.serve_forever()
    except KeyboardInterrupt:
        server.socket.close()

if __name__ == "__main__":
    crawler()
