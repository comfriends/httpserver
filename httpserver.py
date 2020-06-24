from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

import requests
import json
import sys
import datetime
import time

class Elasticsearch:
    def __init__(self, ip, table):
        self.size = 10000
        self.ip = ip
        self.table = table
        self.__scroll = '1m'
        self.__scroll_id = None
        self.__errorCount = 0
        self.__bulksize = 3000
        self.__start = time.time()
        self.__lastday = datetime.datetime.now()

    def runningTime(self):
        return time.time() - self.__start

    def scrollExport(self):
        dd = self.__firstScroll()
        if dd=='complete' : return []
        result = [dd]
        while True:
            dd = self.__next()
            if dd=='complete' : break
            result.append(dd)
        return result

    def __firstScroll(self):
        dt = self.__lastday.strftime("%Y-%m-%d")
        es_url = 'http://' + self.ip + '/' + self.table + '/' + '_search?scroll=1m'
        data = {'size':self.size, 'query':{'bool':{'should':[{'range':{'cnt':{'gte':1}}},{'range':{'logtime':{'gte': dt}}}]}}}
        headers = {'Content-Type': 'application/json'}
        res = requests.post(url=es_url, data=json.dumps(data), headers=headers)

        return self.__parseToBulkFormat(res)

    def __parseToBulkFormat(self, res):
        es_result = res.json()
        if 'error' in es_result:
            print (json.dumps(es_result, indent=2))
            sys.exit(1)

        self.__scroll_id = es_result['_scroll_id']
        es_data = es_result['hits']['hits']


        res_data = []
        for element in es_data :
            element = element['_source']

            dt = datetime.datetime.strptime(element['logtime'], "%Y-%m-%dT%H:%M:%S") + datetime.timedelta(hours=9)
            res_data.append(json.dumps({
                'cnt': element['cnt'],
                'eps': element['eps'],
                'logtime': dt.strftime("%Y-%m-%d %H:%M:%S")
            }));
            self.__lastday = dt

        if res_data == []:
            return 'complete'
        return res_data #''.join(res_data).encode('utf-8')



    def __next(self):
        es_url = 'http://' + self.ip + '/'  +  '_search/scroll'
        data = {'scroll':self.__scroll,'scroll_id':self.__scroll_id}
        headers = {'Content-Type': 'application/json'}
        res = requests.post(url=es_url, data=json.dumps(data), headers=headers)
        print('test1111')
        return self.__parseToBulkFormat(res)


    def deleteScroll(self):
        es_url = 'http://' + self.ip + '/' + '_search/scroll'
        data = {'scroll_id':self.__scroll_id}
        headers = {'Content-Type': 'application/json'}
        res = requests.delete(url=es_url, data=json.dumps(data), headers=headers)

        return res.json()



class CSVHTTPServer(BaseHTTPRequestHandler):
    def do_GET(self):
        es = Elasticsearch('app_elastic_hot:19200', 'dti-system-*')
        res_data = es.scrollExport()

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        for i in range(len(res_data)):
            self.wfile.write(''.join(res_data[i]).encode('utf-8'))

        print(es.runningTime())

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

if __name__ == "__main__":
    server_address = ("0.0.0.0", 19201)
    HTTPServer(server_address, CSVHTTPServer).serve_forever()
    #server = ThreadingHTTPServer(("0.0.0.0", 19201), CSVHTTPServer)
    #server.serve_forever()
