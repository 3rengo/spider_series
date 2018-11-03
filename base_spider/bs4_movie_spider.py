# -*- coding:utf-8 -*-
import csv
import urllib2
import time
import sys 
reload(sys) 
sys.setdefaultencoding("utf-8")

from bs4 import BeautifulSoup

#
# 全局配置
#
# 请求头
HEADER = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
          "Referer": "http://www.dytt8.net/"
          }
# 最新电影获取网站
TARGET_SITE = 'http://www.dytt8.net'
# 电影列表地址，通配链接
MVLIST_URL = TARGET_SITE + "/html/gndy/dyzz/list_23_%d.html"

def get_html_content(url, data=None, header=HEADER):
    """获取网页内容"""
    try:
        request = urllib2.Request(url, data, HEADER)
        resp = urllib2.urlopen(request).read()
    except Exception:
        return ""
    else:
        return resp

def get_mvdetail_links(html):
    """解析html，获取电影详情地址"""
    soup = BeautifulSoup(html, "html5lib", fromEncoding="gb18030")
    try:
        items = soup.find(class_='co_content8').find('ul').find_all('table')
        items = filter(lambda x: bool(x), items)
    except Exception:
        # 未发现电影列表
        return []
    def parse(tag):
        a_tag = tag.find('a', href=True, class_='ulink')
        # print a_tag.string
        return {"mv_name": a_tag.string, 
                "mv_link": TARGET_SITE + a_tag.attrs.get("href")}
    return map(parse, items)

def walk_mvlist():
    """遍历电影列表，解析获取所有电影详情信息，并返回"""
    # 电影详情链接
    mvs = []
    page_num = 1
    while 1:
        aurl = MVLIST_URL % page_num
        print aurl
        html = get_html_content(aurl)
        # 控制爬取速度，过于频繁，容易被禁
        time.sleep(1)
        if html:
            mvs_t = get_mvdetail_links(html)
            mvs += mvs_t
            # mvs_t为[],本网页没有可下载电影,间接获取全部网站详情链接
            # 退出循环
            if not mvs_t: break
        else:
            # 访问网页异常，尝试再访问
            continue
        page_num += 1
        # 仅供学习，增加限制条件，限制获取电影详情页面链接的数量
        if page_num > 5: break
    return mvs


def get_mv_downlink(addr, html):
    """解析html，获取电影下载链接"""
    soup = BeautifulSoup(html, "html5lib", fromEncoding="gb18030")
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

def walk_mvdetail(mvds):
    """遍历电影详情，得到电影对应的下载链接"""
    mvls = []
    for mvd in mvds:
        html = get_html_content(mvd.get('mv_link'))
        # 控制爬取速度，过于频繁，容易被禁
        time.sleep(1)
        if not html:
            # 访问网页异常，尝试再访问
            continue
        # 解析电影详情页面，获取网站下载url
        mv_name, links = get_mv_downlink(mvd.get('mv_link'), html)
        if links:
            mvls.append({"mv_name": mv_name, "mv_dlink": links})
    return mvls

def format_ouput(mvls, fn= '电影大全.csv'):
    """以csv格式，保存电影信息"""
    header = ['电影名称', '电影链接']
    ul = map(lambda x: (x.get("mv_name"), ",".join(x.get("mv_dlink"))), mvls)
    with open(fn, 'w') as f:
        fcsv = csv.writer(f)
        fcsv.writerow(header)
        fcsv.writerows(ul)
    return None

if __name__ == '__main__':
    # 爬取某电影网站，电影对应下载地址. 对应三步走：
    # 1. 获取全部电影详情页面链接地址;
    # 2. 从电影详情页面获取电影下载地址，已知电影详情页面地址;
    # 3. 格式化输出.
    mvds = walk_mvlist()
    mvls = walk_mvdetail(mvds)
    format_ouput(mvls)