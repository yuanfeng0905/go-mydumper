#coding:utf-8

from subprocess import *
import os
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
    p = call(
        ['./mydumper', 
        '-m', 'doris', 
        '-h', '10.8.185.190', 
        '-P', '9030', 
        '-u', 'root',
        '-p', "\!@#\$411589559",
        '-d', './repair_sql',
        '-db', db,
        '-table', table, 
        '-vars', "SET query_timeout=3600;SET exec_mem_limit=10737418240"
        ])
   

def run_loader():
    p = call(
        ['./myloader',
        '-dp', '10.7.51.44:8040,10.7.66.46:8040,10.7.84.112:8040,10.7.187.18:8040',
        '-P', '9030',
        '-d', './repair_sql',
        '-h', '10.7.85.221',
        '-m', 'doris',
        '-u', 'root',
        '-p', '123456',
        '-t', 8
        ])


def do():
    conn = Conn(host='10.8.185.190', port=9030, user='root', passwd='!@#$411589559')
    cur = conn.cursor()
    # 检查所有含有decimal类型的表

    # for loader
    cur.execute("select TABLE_SCHEMA,table_name from information_schema.columns where column_type like 'decimal%' group by table_schema,table_name;")
    for db, table in cur.fetchall():
        run_dumper(db, table)

    # for loader
    run_loader()

    cur.close()
    conn.close()


if __name__ == '__main__':
    do()
