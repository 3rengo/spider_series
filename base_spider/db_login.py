# !/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Description: 模拟页面登录
@Site: www.3sanrenxing.com
@Author: 皮匠
'''
from bs4 import BeautifulSoup
import time
import requests


def get_hiddentag_value(sess, login_url):
    r = sess.get(login_url)
    soup = BeautifulSoup(r.content, "html5lib")
    hidden_tags = soup.select_one("#lzform").find_all('input', attrs={"type": "hidden"})
    hidden_form = {}
    for tg in hidden_tags:
        hidden_form[tg.attrs.get("name")] = tg.attrs.get("value")
    return hidden_form


def do_login_test():
    email = raw_input('please enter email:')
    pw = raw_input('please enter pw:')
    login_url = 'https://accounts.douban.com/login'
    sess = requests.session()
    sess.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36'}
    hidden_form = get_hiddentag_value(sess, login_url)
    form_data = {'form_email': email, 'form_password': pw}
    form_data.update(hidden_form)
    r = sess.post(login_url, data=form_data)
    print r.status_code
    return sess


def test(session):
    """验证是否成功
    带有cookie信息session对象，访问只有用户登录才能访问的页面
    """
    # 豆瓣个人消息页面
    r = session.get('https://www.douban.com/doumail/')
    print 'status code: ', r.status_code
    with open('doumail.html', 'w') as f:
        f.write(r.content)
    soup = BeautifulSoup(r.content, "html5lib")
    tag1 = soup.select_one('#db-global-nav > div > div.top-nav-info')
    print tag1


def main():
    sess = do_login_test()
    test(sess)


if __name__ == "__main__":
    main()
