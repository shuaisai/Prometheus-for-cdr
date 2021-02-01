import os
import sys
import datetime
import time
import redis

def WtcCount(filename):

    wtccount = {'total': 0,
                'answer': 0}

    with open(filename, 'r') as fp:

        try:

            for line in fp:
                cdrlist = line.split(',')

                during = cdrlist[28]
                wtccount['total'] += 1

                if int(during) > 0:
                    wtccount['answer'] += 1
        except:
            pass

    return wtccount

if __name__ == '__main__':

    Nfilename = ''

    while True:

        day = datetime.datetime.now().strftime("%d")

        dir = f"/uloc/wtc/XDR/backup/CDR/{day}"
        try:
            file = os.popen("ls -t %s|head -n1|awk '{print $0}'" % dir).read()
            filename = dir + '/' + file
            filename = filename.strip()
        except:
            filename = Nfilename

        if filename != Nfilename:
            Nfilename = filename
            cdrtime = filename.split('/')[-1].split('_')[-1].split('.')[0]

            re = WtcCount(filename)

            pool = redis.ConnectionPool(host='192.168.10.56', port=6379, db=6)
            r = redis.Redis(connection_pool=pool)

            r.set(f'ASC56:wtc:total:{cdrtime}', re['total'], ex=300)
            r.set(f'ASC56:wtc:answer:{cdrtime}', re['answer'], ex=300)

            r.connection_pool.disconnect()

        time.sleep(30)