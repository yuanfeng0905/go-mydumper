#!python

#coding:utf-8

from subprocess import *
from pymysql.connections import Connection

def Conn(host=None, port=None, user=None, passwd=None):
    _connection_settings = {}
    _connection_settings['host'] = host
    _connection_settings['port'] = port
    _connection_settings['user'] = user
    _connection_settings['passwd'] = passwd
    _connection_settings['use_unicode'] = True
    _connection_settings['charset'] = 'utf8'
    conn = Connection(**_connection_settings)
    conn.autocommit(True)

    return conn


def run_dumper(db, table):
    p = Popen(
        ['./mydumper', 
        '-h', '10.8.185.190', 
        '-P', 9030, 
        '-u', 'root',
        '-p', '!@#$411589559',
        '-d', './sql_'+db,
        '-db', db,
        '-table', table, 
        '-vars', "SET query_timeout=3600;SET exec_mem_limit=10737418240"
        ],
        stdin=PIPE,
        stdout=PIPE,
        stderr=PIPE)
    p.wait()
    out = p.stdout.read()
    print(">>", out)


def do():
    conn = Conn(host='10.8.185.190', port=9030, user='root', passwd='!@#$411589559')
    cur = conn.cursor()
    # 检查所有含有decimal类型的表

    cur.execute("select TABLE_SCHEMA,table_name from columns where column_type like 'decimal%' group by table_schema,table_name;")
    for db, table in cur.fetchall():
        run_dumper(db, table)
    cur.close()
    conn.close()


if __name__ == '__main__':
    do()
