import os
import sys
import datetime
import time
import redis
import importlib

if sys.getdefaultencoding() != 'utf-8':
    importlib.reload(sys)
    sys.setdefaultencoding('utf-8')

def CdrCount(filename):

    cdrbind = {}
    cdrunbind = {}

    try:
        with open(filename, 'r') as fp:

                for line in fp:
                    cdrlist = line.split(',')
                    apk = cdrlist[5]
                    mode = cdrlist[4]

                    if int(mode) == 21:
                        if apk in cdrbind:
                            cdrbind[apk] += 1
                        else:
                            cdrbind[apk] = 1
                    elif int(mode) == 22:
                        if apk in cdrunbind:
                            cdrunbind[apk] += 1
                        else:
                            cdrunbind[apk] = 1
    except:
        pass

    return cdrbind, cdrunbind

if __name__ == '__main__':

    Nfilename = ''

    while True:

        day = datetime.datetime.now().strftime("%d")
        if list(day)[0] == '0':
            day = list(day)[1]
        dir = f"/uloc/XDR/backup/{day}"
        try:
            file = os.popen("ls -t %s|head -n1|awk '{print $0}'" % dir).read()
            if file:
                filename = dir + '/' + file
                filename = filename.strip()
            else:
                filename = Nfilename
        except:
            pass

        # filename = r'C:\Users\15807\Desktop\XDR_SUBDR_202102051246.txt'

        if filename != Nfilename:
            Nfilename = filename
            cdrtime = filename.split('/')[-1].split('_')[-1].split('.')[0]

            re = CdrCount(filename)
            print(re)

            pool = redis.ConnectionPool(host='192.168.10.56', port=6379, db=6)
            r = redis.Redis(connection_pool=pool)

            for key in re[0]:
                r.set(f'ASC14:bind:{cdrtime}:{key}', re[0][key], ex=300)

            for key in re[1]:
                r.set(f'ASC14:unbind:{cdrtime}:{key}', re[1][key], ex=300)

            r.connection_pool.disconnect()

        time.sleep(30)