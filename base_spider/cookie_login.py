#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Description: cookie登录
@Site: www.3sanrenxing.com
@Author: 皮匠
'''
import time

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import requests


def get_cookies():
    """通过selenium模拟浏览器登录，得到cookie"""
    email = raw_input('please enter email:')
    pw = raw_input('please enter password:')
    driver = webdriver.Chrome()
    driver.get("https://www.douban.com/")
    # 输入邮箱
    driver.find_element_by_xpath('//*[@id="form_email"]').send_keys(email)
    # 输入密码
    driver.find_element_by_xpath('//*[@id="form_password"]').send_keys(pw)
    is_vercode = raw_input("是否有验证码[yes/no]:")
    if is_vercode == 'yes':
        ver_code = raw_input("please enter ver code:")
        driver.find_element_by_xpath('//*[@id="captcha_field"]').send_keys(ver_code)
    # 表单submit
    driver.find_element_by_xpath('//*[@id="lzform"]').submit()
    driver.get("https://movie.douban.com")
    return driver.get_cookies()


def init_session(cookies):
    """将登录的cookie信息，添加到session对象中"""
    sess = requests.session()
    sess.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36'}
    cookieJar = requests.cookies.RequestsCookieJar()
    [cookieJar.set(ck.get("name"), ck.get("value"), domain=ck.get("domain")) for ck in cookies]
    # 将cookiesJar赋值给会话
    sess.cookies = cookieJar
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
    cookies = get_cookies()
    sess = init_session(cookies)
    test(sess)

if __name__ == "__main__":
    main()
