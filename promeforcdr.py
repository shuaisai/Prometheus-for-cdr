#! /usr/bin/python
# -*- coding: utf-8 -*-
#auther: shuaisai

import redis
import datetime
import prometheus_client
from flask import Response, Flask
from prometheus_client import CollectorRegistry, Gauge, Counter
import configparser

def RedisGet(asclist, apklist, redisip, cdrtime, asalist):
    pool = redis.ConnectionPool(host=redisip, port=6379, db=6, decode_responses=True)
    r = redis.Redis(connection_pool=pool)

    totallist = {}
    answerlist = {}
    wtclist = {}
    wtctotal = 0
    wtcanswer = 0

    for apk in apklist:
        cdrtotal = 0
        for asc in asclist:
            v = r.get(f'{asc}:total:{cdrtime}:{apk}')
            if v == None:
                v = 0
            v = int(v)
            cdrtotal += v
            totallist[apk] = cdrtotal


    for apk in apklist:
        cdranswer = 0
        for asc in asclist:
            v = r.get(f'{asc}:answer:{cdrtime}:{apk}')
            if v == None:
                v = 0
            v = int(v)
            cdranswer += v
            answerlist[apk] = cdranswer

    for wtcasc in asalist:
        v1 = r.get(f'{wtcasc}:wtc:total:{cdrtime}')
        print(f"{wtcasc}:wtc:total:{cdrtime}")
        v2 = r.get(f'{wtcasc}:wtc:answer:{cdrtime}')
        print(f"v1,v2:{v1}{v2}")
        if v1 == None:
            v1 = 0
        v1 = int(v1)
        wtctotal += v1

        if v2 == None:
            v2 = 0
        v2 = int(v2)
        wtcanswer += v2

        wtclist['total'] = wtctotal
        wtclist['answer'] = wtcanswer

    r.connection_pool.disconnect()

    print(totallist, answerlist, wtclist)

    return totallist, answerlist, wtclist



# asclist = ['ASC15', 'ASC16', 'ASC21', 'ASC22']
# apklist = ['ZHXS','NBGYXS','NBGYSK','TRRTCC','XAYY','SZZTCS','HZWLJY','ZJXYCS','ZHVIP','HWHSX']
# asalist = ['ASC56', 'ASC57']

def GetConfig():
    print(datetime.datetime.now(), '正在读取配置文件')
    config = configparser.ConfigParser()
    config_path = r'./config.ini'
    config.read(config_path, encoding='utf-8')
    asclist1 = config.get('config', 'asclist')
    asclist = asclist1.split(',')
    apklist1 = config.get('config', 'apklist')
    apklist = apklist1.split(',')
    asalist1 = config.get('config', 'asalist')
    asalist = asalist1.split(',')
    return asclist, apklist, asalist


app = Flask(__name__)

@app.route("/metrics")
def r_value():
    registry = CollectorRegistry()

    cdrcount = Gauge('cdr_count', 'count of call cdr', ['status'], registry=registry)
    monitor_state = Gauge("process_monitor_up", "state of process monitor", registry=registry)
    apkcount = Gauge('cdr_count_apk', 'count of apk call cdr', ['apk', 'status'], registry=registry)
    wtccount = Gauge('cdr_count_wtc', 'count of wtc call cdr', ['status'], registry=registry)

    monitor_state.set('1')

    cdrtime = (datetime.datetime.now() - datetime.timedelta(minutes=2)).strftime("%Y%m%d%H%M")
    print(cdrtime)

    list = GetConfig()


    cdrdata = RedisGet(asclist=list[0], apklist=list[1], redisip='192.168.10.56', cdrtime=cdrtime, asalist=list[2])

    d1 = 0
    d2 = 0

    for k in cdrdata[0]:
        d1 += int(cdrdata[0][k])

    for k in cdrdata[1]:
        d2 += int(cdrdata[1][k])



    cdrcount.labels(status='total').set(d1)
    cdrcount.labels(status='answer').set(d2)

    for apk in list[1]:
        apkcount.labels(status='total', apk=apk).set(cdrdata[0][apk])
        apkcount.labels(status='answer', apk=apk).set(cdrdata[1][apk])

    wtccount.labels(status='total').set(cdrdata[2]['total'])
    wtccount.labels(status='answer').set(cdrdata[2]['answer'])

    return Response(prometheus_client.generate_latest(registry),
                    mimetype="text/plain")

if __name__ == "__main__":
    app.run(host="127.0.0.1", port='9250')



