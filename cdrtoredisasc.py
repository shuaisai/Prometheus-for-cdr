import os
import sys
import datetime
import time
import redis

def CdrCount(filename):

    cdrcount = {}
    cdranswer = {}

    try:
        with open(filename, 'r') as fp:

                for line in fp:
                    cdrlist = line.split(',')
                    apk = cdrlist[5]
                    during = cdrlist[28]

                    if apk in cdrcount:
                        cdrcount[apk] += 1
                    else:
                        cdrcount[apk] = 1

                    if int(during) > 0:
                        if apk in cdranswer:
                            cdranswer[apk] += 1
                        else:
                            cdranswer[apk] = 1
    except:
        pass

    return cdrcount, cdranswer

if __name__ == '__main__':

    Nfilename = ''

    while True:

        day = datetime.datetime.now().strftime("%d")
        if list(day)[0] == '0':
            day = list(day)[1]
        dir = f"/uloc/wt/XDR/backup/CDR/{day}"
        try:
            file = os.popen("ls -t %s|head -n1|awk '{print $0}'" % dir).read()
            if file:
                filename = dir + '/' + file
                filename = filename.strip()
            else:
                filename = Nfilename
        except:
            pass

        if filename != Nfilename:
            Nfilename = filename
            cdrtime = filename.split('/')[-1].split('_')[-1].split('.')[0]

            re = CdrCount(filename)

            pool = redis.ConnectionPool(host='192.168.10.56', port=6379, db=6)
            r = redis.Redis(connection_pool=pool)

            for key in re[0]:
                r.set(f'ASC21:total:{cdrtime}:{key}', re[0][key], ex=300)

            for key in re[1]:
                r.set(f'ASC21:answer:{cdrtime}:{key}', re[1][key], ex=300)

            r.connection_pool.disconnect()

        time.sleep(30)