# -*- coding:utf-8 -*-
import urllib2
import time
import sys
import os
reload(sys) 
sys.setdefaultencoding("utf-8")

from bs4 import BeautifulSoup


def get_html_content(url, data=None):
    header = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36"}
    """获取网页内容"""
    try:
        request = urllib2.Request(url, data, header)
        resp = urllib2.urlopen(request).read()
    except Exception:
        return ""
    else:
        return resp


def get_mvdetail_links(html):
    """解析html，获取电影详情地址"""
    soup = BeautifulSoup(html, "html5lib", from_encoding="gb18030")
    try:
        items = soup.find(class_='co_content8').find('ul').find_all('table')
        items = filter(lambda x: bool(x), items)
    except Exception:
        # 未发现电影列表
        return []
    def parse(tag):
        a_tag = tag.find('a', href=True, class_='ulink')
        return (a_tag.string, TARGET_SITE + a_tag.attrs.get("href"))
    return map(parse, items)


def walk_mvlist(mvlist_link_tmpl, links_num=100):
    """遍历电影列表，获取电影详情链接"""
    # 电影详情链接
    mvs = []
    start_page = 1
    while len(mvs) < links_num:
        link = mvlist_link_tmpl % start_page
        print '\033[1;33;44m %s \033[0m' % link
        html = get_html_content(link)
        # 控制爬取速度，过于频繁，容易被禁
        time.sleep(1)
        if html:
            mvs_c = get_mvdetail_links(html)
            mvs += mvs_c
            # mvs_t为[],说明该网页没有电影链接,间接判断已经抓取全部链接
            # 结束循环
            if not mvs_c: break
        else:
            # 访问网页异常，尝试再访问
            continue
        start_page += 1
    return mvs[:links_num]


def get_mv_downlink(html):
    """解析html，获取电影下载链接"""
    otherkeys = {'oncontextmenu', 'thunderrestitle', 'target', 'href', 'onclick', 'thunderpid', 'thundertype'}
    soup = BeautifulSoup(html, "html5lib", from_encoding="gb18030")
    a_tags = soup.select('#Zoom > span > table > tbody > tr > td > a')
    def _parse_link(tag):
        attr = set(tag.attrs.keys()) - otherkeys
        if len(attr) > 0:
            attr = attr.pop()
            return tag.attrs.get(attr)
        else:
            return None
    links = map(_parse_link, a_tags)
    links = filter(lambda x: bool(x), links)
    return links


def walk_mvdetail(mvds, driver):
    """遍历电影详情链接，获得电影下载链接"""
    mvs = []
    for mdl in mvds:
        # 打开电影详情页面
        driver.get(mdl[1])
        # 解析电影详情页面，获取网站下载url
        links = get_mv_downlink(driver.page_source)
        print '\033[1;33;44m %s \033[0m' % links
        if links:
            mvs.append((mdl[0], links))
        # 控制爬取速度，过于频繁，容易被禁
        time.sleep(1)
    return mvs


def save_file(mvs, fp='./', fn=u'电影列表.txt'):
    """保存电影下载链接"""
    rows = [(u'电影名称', [u'电影链接'])] + mvs
    fpname = os.path.join(fp, fn)
    with open(fpname, 'w') as f:
        rows = map(lambda x: "%s, %s" % (x[0], ",".join(x[-1])), rows)
        f.write(u"\r\n".join(rows))
    return fpname


def create_webdriver():
    """创建一个Chrome web driver实例"""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    options = Options()
    # 加上这个属性来规避bug
    options.add_argument('--disable-gpu')
    # 不加载图片, 提升速度
    options.add_argument('blink-settings=imagesEnabled=false')
    # 浏览器不提供可视化页面
    options.add_argument('--headless')
    return webdriver.Chrome(chrome_options=options)


# 电影网站
TARGET_SITE = 'http://www.dytt8.net'
def spider_main():
    # 电影列表通配链接
    mvlist_url = TARGET_SITE + "/html/gndy/dyzz/list_23_%d.html"
    mvds = walk_mvlist(mvlist_url, 50)
    driver = create_webdriver()
    mvs = walk_mvdetail(mvds, driver)
    print u'\033[1;33;44m保存: %s \033[0m' % save_file(mvs)


if __name__ == '__main__':
    spider_main()
