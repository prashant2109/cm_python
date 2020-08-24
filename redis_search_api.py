from redisearch import Client, TextField, NumericField, Query, Result, AutoCompleter, Suggestion
import ConfigParser
import config
import redis
import re
import os

class search:
    def __init__(self):
        self.redis_info = config.Config.redis_info
        self.ip, self.port, self.db = self.redis_info["host"], self.redis_info["port"], self.redis_info["db"]
        index_name = self.redis_info["tb_name"]
        self.client = Client(index_name, self.ip, self.port)
        #self.rd_con = self.make_redis_connection()
        self.escape1 = re.compile(r'&#\d+;')
        self.escape2 = re.compile(r',|\.|<|>|{|}|[|]|"|\'|:|;|!|@|#|\$|%|\^|&|\*|\(|\)|-|\+|=|~')
        self.escape3 = re.compile(r'\s+')
        pass

    def StringEscape(self, search_str):
        search_str = re.sub(self.escape1, '', search_str)
        search_str = re.sub(self.escape2, '', search_str)
        search_str = re.sub(self.escape3, ' ', search_str)
        return search_str.strip()

    def make_redis_connection(self):
        ip,port,db = self.config.get('redis_search', 'storage').split('##')
        self.ip = ip
        self.port = port
        redis_conn = redis.StrictRedis(host=ip, port=str(port), db=str(db))
        return redis_conn

    def search_exact_Query(self, string):
        string = self.StringEscape(string)
        query = "(@look_cmp:%s*)|(@cmp_k:%s*)"%(string, string)
        res = self.client.search(Query(query).paging(0, 10000))
        arr = []
        for x in res.docs:
            arr.append({"k": x.cmp_k, "n": x.cmp_name})
        arr.sort(key=lambda x:len(x['n']))
        return [{"message": "done", "data": arr}]

'''if __name__ == '__main__':
    path = os.getcwd()+"/Config.ini"
    obj = search(path, 'cmp_management')
    string = "au"
    string = obj.StringEscape(string)
    query = "@look_cmp:%s*"%(string)
    print obj.search_exact_Query(query)'''
