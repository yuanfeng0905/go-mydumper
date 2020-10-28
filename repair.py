#coding:utf-8

from subprocess import *


def do():
    lines = []
    cache = {}
    with open('error.log') as ef:
        for l in ef.readlines():
            if l in cache:
                continue
            
            db, tb = l.split('.')
            lines.append('./mydumper -m doris -h 10.8.185.190 -t 16 -P 9030 -u root -p \!@#\$411589559 -d ./repair_sql -db %s -table %s -vars "SET query_timeout=3600;SET exec_mem_limit=10737418240"' % (db.strip(), tb.strip()))
            cache[l] = ''
        
    with open('repair_bash.sh', 'w+') as f:
        f.write('\n'.join(lines))

if __name__ == '__main__':
    do()
