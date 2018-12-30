#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Description: 代理爬虫
@Site: www.3sanrenxing.com
@Author: 皮匠
'''
from bs4 import BeautifulSoup
import requests
import time


class HttpProxyPool(object):
    def __init__(self, zq_page_num=1, max_resp_time=700, timeout=2):
        """
        :zq_page_num, 每个提供http代理机构，抓取多少页代理ip数据，默认5页
        :min_resp_time, 对代理请求，最大响应时间，单位毫秒，默认500ms
        :timeout, 请求超时时间，单位秒，默认5s
        """
        self._proxy_source = {
            'xicidaili': [ 'https://www.xicidaili.com/nn/%s' % i for i in range(1, zq_page_num+1)],
            'kuaidaili': [ 'https://www.kuaidaili.com/free/inha/%s/' % i for i in range(1, zq_page_num + 1)],
            'qydaili': ['http://www.qydaili.com/free/?page=%s' % i for i in range(1, zq_page_num + 1)],
        }
        self._max_resp_time = max_resp_time
        self._timeout = timeout
        # 类型http or https,ip:port
        self._ippool = {'HTTP': [],'HTTPS':[]}
        self._sess = requests.session()
        self._sess.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}
        # 解析代理ip
        self._extract_ip()
        # 验证代理ip有效性
        self._check_proxy()

    def update_ip(self, zq_page_num=1):
        self._proxy_source = {
            'xicidaili': [ 'https://www.xicidaili.com/nn/%s' % i for i in range(1, zq_page_num+1)],
            'kuaidaili': [ 'https://www.kuaidaili.com/free/inha/%s/' % i for i in range(1, zq_page_num + 1)],
            'qydaili': ['http://www.qydaili.com/free/?page=%s' % i for i in range(1, zq_page_num + 1)],
        }
        # 解析代理ip
        self._extract_ip()
        # 验证代理ip有效性
        self._check_proxy()

    def _extract_ip(self):
        for source, urls in self._proxy_source.items():
            for url in urls:
                r = self._sess.get(url)
                if not r.ok: continue
                self._get_parse_fn(source, r.content)
        # 去重
        self._ippool['HTTP'] = list(set(self._ippool['HTTP']))
        self._ippool['HTTPS'] = list(set(self._ippool['HTTPS']))

    def _qydaili_parse_fn(self, html):
        soup = BeautifulSoup(html, "html5lib")
        tags = soup.select_one('#content > section > div.container > table > tbody').find_all('tr')
        for tag in tags:
            td_tags = tag.find_all('td')
            # pos: 0->ip 1->port 2->代理类型 3-> http or https
            if u'高匿' != td_tags[2].get_text():
                continue
            self._ippool[td_tags[3].get_text()].append(
                '%s:%s' % (td_tags[0].get_text(), td_tags[1].get_text()))

    def _xicidaili_parse_fn(self, html):
        soup = BeautifulSoup(html, "html5lib")
        tags = soup.select_one('#ip_list > tbody').find_all(
            'tr', attrs={"class": "odd"})
        for tag in tags:
            td_tags = tag.find_all('td')
            # pos: 1->ip 2->port 4->代理类型 5-> http or https
            if u'高匿' != td_tags[4].get_text(): continue
            self._ippool[td_tags[5].get_text()].append('%s:%s' % (td_tags[1].get_text(), td_tags[2].get_text()))

    def _kuaidaili_parse_fn(self, html):
        soup = BeautifulSoup(html, "html5lib")
        tags = soup.select_one('#list > table > tbody').find_all('tr')
        for tag in tags:
            td_tags = tag.find_all('td')
            # pos: 0->ip 1->port 2->代理类型 3-> http or https
            if u'高匿名' != td_tags[2].get_text():
                continue
            self._ippool[td_tags[3].get_text()].append('%s:%s' % (td_tags[0].get_text(), td_tags[1].get_text()))

    def _get_parse_fn(self, source, html):
        time.sleep(1)
        if 'xicidaili' == source:
            self._xicidaili_parse_fn(html)
        elif 'kuaidaili' == source:
            self._kuaidaili_parse_fn(html)
        elif 'qydaili' == source:
            self._qydaili_parse_fn(html)
        return

    def get_proxyip(self):
        print 'HTTPS PROXY:', len(self._ippool['HTTPS'])
        print 'HTTP PROXY:', len(self._ippool['HTTP'])
        return self._ippool.copy()

    def check_http(self, ipstr):
        st = time.time()
        try:
            r = self._sess.get(url='http://www.baidu.com',
                               timeout=self._timeout, proxies={"http": "http://" + ipstr})
        except:
            return False, ipstr
        else:
            usage_time = int((time.time() - st) * 100)
            if usage_time > self._max_resp_time:
                return False, ipstr
            print ipstr, r.ok, usage_time
            return True, ipstr

    def check_https(self, ipstr):
        st = time.time()
        try:
            r = self._sess.get(url='https://www.baidu.com',
                               timeout=self._timeout, proxies={"https": "http://" + ipstr})
        except:
            return False, ipstr
        else:
            usage_time = int((time.time() - st) * 100)
            if usage_time > self._max_resp_time:
                return False, ipstr
            print ipstr, r.ok, usage_time
            return True, ipstr

    def _check_proxy(self):
        res = [ self.check_http(ipstr) for ipstr in self._ippool['HTTP'] ]
        map(lambda x: x[0] or self._ippool['HTTP'].remove(x[1]), res)
        res = [ self.check_https(ipstr) for ipstr in self._ippool['HTTPS'] ]
        map(lambda x: x[0] or self._ippool['HTTPS'].remove(x[1]), res)
        return None


def test_spider():
    """代理测试"""
    import json
    hpp = HttpProxyPool()
    proxyip = hpp.get_proxyip()
    print "HTTP PROXY"
    for ip_str in proxyip['HTTP']:
        try:
            r = requests.get('http://httpbin.org/get',
                        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'},
                        proxies={"http": "http://" + ip_str})
            resp = json.loads(r.content)
            print u'代理地址: %s, 服务端监测到的地址: %s!' % (ip_str, resp['origin'])
        except:
            print "FATAL PROXY IP:", ip_str
    print "HTTPS PROXY"
    for ip_str in proxyip['HTTPS']:
        try:
            r = requests.get('https://httpbin.org/get',
                        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'},
                        proxies={"https": "http://" + ip_str})
            resp = json.loads(r.content)
            print u'代理地址: %s, 服务端监测到的地址: %s!' % (ip_str, resp['origin'])
        except:
            print "FATAL PROXY IP:", ip_str


if __name__ == "__main__":
    test_spider()
