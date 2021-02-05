#! /usr/bin/python
# -*- coding: utf-8 -*-
#auther: shuaisai

import redis
import datetime
import prometheus_client
from flask import Response, Flask
from prometheus_client import CollectorRegistry, Gauge, Counter
import configparser

def RedisGet(asclist, apklist, redisip, cdrtime, asalist, bsslist):
    pool = redis.ConnectionPool(host=redisip, port=6379, db=6, decode_responses=True)
    r = redis.Redis(connection_pool=pool)

    totallist = {}
    answerlist = {}
    bindlist = {}
    unbindlist = {}
    wtclist = {}
    wtctotal = 0
    wtcanswer = 0

    for apk in apklist:
        cdrtotal = 0
        cdranswer = 0
        cdrbind = 0
        cdrunbind = 0

        for asc in asclist:
            v1 = r.get(f'{asc}:total:{cdrtime}:{apk}')
            if v1 == None:
                v1 = 0
            v1 = int(v1)
            cdrtotal += v1
            totallist[apk] = cdrtotal

            v2 = r.get(f'{asc}:answer:{cdrtime}:{apk}')
            if v2 == None:
                v2 = 0
            v2 = int(v2)
            cdranswer += v2
            answerlist[apk] = cdranswer

        for bss in bsslist:
            v3 = r.get(f'{bss}:bind:{cdrtime}:{apk}')
            if v3 == None:
                v3 = 0
            v3 = int(v3)
            cdrbind += v3
            bindlist[apk] = cdrbind

            v4 = r.get(f'{bss}:unbind:{cdrtime}:{apk}')
            if v4 == None:
                v4 = 0
            v4 = int(v4)
            cdrunbind += v4
            unbindlist[apk] = cdrunbind

    for wtcasc in asalist:
        v1 = r.get(f'{wtcasc}:wtc:total:{cdrtime}')
        v2 = r.get(f'{wtcasc}:wtc:answer:{cdrtime}')
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

    # print(bindlist, unbindlist)

    return totallist, answerlist, wtclist, bindlist, unbindlist



# asclist = ['ASC15', 'ASC16', 'ASC21', 'ASC22']
# apklist = ['ZHXS','NBGYXS','NBGYSK','TRRTCC','XAYY','SZZTCS','HZWLJY','ZJXYCS','ZHVIP','HWHSX']
# asalist = ['ASC56', 'ASC57']

def GetConfig():
    print(datetime.datetime.now(), '正在读取配置文件')
    config = configparser.ConfigParser()
    config_path = r'./config.ini'
    config.read(config_path, encoding='utf-8')
    asclist = config.get('config', 'asclist').split(',')
    apklist = config.get('config', 'apklist').split(',')
    asalist = config.get('config', 'asalist').split(',')
    bsslist = config.get('config', 'bsslist').split(',')
    return asclist, apklist, asalist, bsslist


app = Flask(__name__)

@app.route("/metrics")
def r_value():
    registry = CollectorRegistry()

    cdrcount = Gauge('cdr_count', 'count of call cdr', ['status'], registry=registry)
    monitor_state = Gauge("process_monitor_up", "state of process monitor", registry=registry)
    apkcount = Gauge('cdr_count_apk', 'count of apk call cdr', ['apk', 'status'], registry=registry)
    wtccount = Gauge('cdr_count_wtc', 'count of wtc call cdr', ['status'], registry=registry)
    bsscount = Gauge('cdr_count_bind', 'count of apk bind cdr', ['apk', 'mode'], registry=registry)

    monitor_state.set('1')

    cdrtime = (datetime.datetime.now() - datetime.timedelta(minutes=2)).strftime("%Y%m%d%H%M")
    print(cdrtime)

    list = GetConfig()


    cdrdata = RedisGet(asclist=list[0], apklist=list[1], redisip='192.168.10.56', cdrtime=cdrtime,
                       asalist=list[2], bsslist=list[3])

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
        bsscount.labels(mode='bind', apk=apk).set(cdrdata[3][apk])
        bsscount.labels(mode='unbind', apk=apk).set(cdrdata[4][apk])

    wtccount.labels(status='total').set(cdrdata[2]['total'])
    wtccount.labels(status='answer').set(cdrdata[2]['answer'])

    return Response(prometheus_client.generate_latest(registry),
                    mimetype="text/plain")

if __name__ == "__main__":
    app.run(host="127.0.0.1", port='9250')



