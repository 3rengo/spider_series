#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Description: 基于redis bitmap位数组数据类型，实现布隆过滤器
@Site: www.3sanrenxing.com
@Author: 皮匠
'''
import mmh3
import redis


class RedisBloomFilter(object):
    """基于Redis布隆过滤器实现
    利用Redis bitmap数据结构
    """
    hashfunc_num = 10 # 哈希函数个数
    bitmap_max_offset = 536870911  # bitmap最大偏移量

    def __init__(self, capacity=100, ns='BLOOM_BITMAP', salt='0',
                    rhost='localhost', rport=6379, rdb=1, rpwd=""
                    ):
        """构造器
        :param capacity: int, 布隆过滤器容纳元素个数
        :param ns: string, 指定布隆过滤器名字空间，不同应用环境名字空间值不同
        :param salt: string，盐值,可以走默认的
        :param rhost: string，redis服务所在主机IP
        :param rport: int, redis服务端口
        :param rdb: int，redis服务数据库编号，0~15
        :param rpwd: string，redis服务链接密码
        """
        self._salt = str(salt)
        self._rc = redis.StrictRedis(connection_pool=redis.ConnectionPool(host=rhost, port=rport, db=rdb, password=rpwd))
        self._bitmap_num, self._bitmap_offset = self._get_bitmap_quota(capacity)
        self._bitmaps = {str(i): ns + '_' + str(i) for i in range(self._bitmap_num)}
        self._init_bitmap()

    def __del__(self):
        if self._rc:
            self._rc.connection_pool.disconnect()

    def _get_bitmap_quota(self, capacity):
        """获取bitmap数据结构配置信息
        Return :bitmap数据结构个数,bitmap长度
        :param capacity: int, 布隆过滤器容纳元素个数
        """
        offset = capacity * 20.0
        num = 1 + 1 if offset <= RedisBloomFilter.bitmap_max_offset else round(offset / RedisBloomFilter.bitmap_max_offset) + 1
        offset = offset + 1 if offset <= RedisBloomFilter.bitmap_max_offset else RedisBloomFilter.bitmap_max_offset
        return num, int(offset)

    def _root_hash(self, ele, salt):
        """根hash函数"""
        return abs(mmh3.hash(ele + salt))

    def hash(self, ele):
        """计算字符串hash值"""
        hash_vals = [self._root_hash(str(ele), self._salt * i)
                        % RedisBloomFilter.bitmap_max_offset
                     for i in range(1, RedisBloomFilter.hashfunc_num + 1)]
        return hash_vals

    def _init_bitmap(self):
        """初始化Redis bitmap数据结构"""
        [
            self._rc.setbit(key, self._bitmap_offset, 0)
            for key in self._bitmaps.values()
        ]

    def add(self, ele):
        """向布隆过滤器，添加元素"""
        hash_vals = self.hash(ele)
        [
            self._rc.setbit(self._bitmaps[str(hv % self._bitmap_num)], hv, 1)
            for hv in hash_vals
        ]

    def clear(self):
        """布隆过滤器恢复初始状态"""
        [self._rc.delete(key) for key in self._bitmaps.values()]
        self._init_bitmap()

    def __contains__(self, ele):
        """判断元素是否在布隆过滤器中"""
        hash_vals = self.hash(ele)
        res = [
            self._rc.getbit(self._bitmaps[str(hv % self._bitmap_num)], hv)
            for hv in hash_vals
        ]
        return False if 0 in res else True


def test():
    """测试+使用"""
    rbf = RedisBloomFilter(1000, ns='test-namespace',
                           rhost='10.255.80.64', rport=6400)
    # rbf.clear()
    import uuid
    uuids = [uuid.uuid1() for i in range(150)]
    # 添加元素
    [rbf.add(ele) for ele in uuids]
    for ele in uuids:
        # 判断元素在不在
        print ele, 'exist' if ele in rbf else 'no exists'
    uuid1s = [uuid.uuid1() for i in range(150, 300)]
    for ele in uuid1s:
        # 判断元素在不在
        print ele, 'exist' if ele in rbf else 'no exists'


if __name__ == "__main__":
    test()
