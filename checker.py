#coding:utf-8

from contextlib import contextmanager
from pymysql.connections import Connection
import click
import os

_new_conn = {}
_old_conn = {}


@contextmanager
def get_doris_cur(conn):
    """ 获取 doris 的写连接 """
    _connection_settings = {}
    _connection_settings['host'] = conn['host']
    _connection_settings['port'] = conn['port']
    _connection_settings['user'] = conn['username']
    _connection_settings['passwd'] = conn['password']
    _connection_settings['use_unicode'] = True
    _connection_settings['charset'] = 'utf8'
    c = Connection(**_connection_settings)
    c.autocommit(True)

    cur = c.cursor()
    yield cur
    cur.close()
    c.close()


def all_tables(db):
    tbs = []
    with get_doris_cur(_old_conn) as cur:
        cur.execute('use {}'.format(db))
        cur.execute('show tables')
        for l in cur.fetchall():
            tbs.append(l[0])
    return tbs


def check(db, table):
    global _new_conn, _old_conn
    with get_doris_cur(_old_conn) as old_cur:
        old_cur.execute('select count(*) from {db}.{tb}'.format(db=db,
                                                                tb=table))
        old_cnt = int(old_cur.fetchone()[0])
        print("old db={} table={} count={}".format(db, table, old_cnt))
        try:
            with get_doris_cur(_new_conn) as new_cur:
                new_cur.execute('select count(*) from {db}.{tb}'.format(
                    db=db, tb=table))
                new_cnt = int(new_cur.fetchone()[0])
                print("new db={} table={} count={}".format(db, table, new_cnt))
        except Exception as e:
            new_cnt = 0
            print("query new {}.{} fail:{}".format(db, table, e))

        if old_cnt - new_cnt > 1000:
            print("=======> need recovery {}.{}".format(db, table))
            return True


def escape(s):
    return s.replace('!', "\!").replace('$', "\$")


def load(dir):
    global _new_conn
    cmd = './myloader -dp 10.7.51.44:8040,10.7.66.46:8040,10.7.84.112:8040,10.7.187.18:8040 -P {port} -d {dir} -h {host} -m doris -u {user} -p {password} -t 8'.format(
        dir=dir,
        host=_new_conn['host'],
        port=_new_conn['port'],
        user=_new_conn['username'],
        password=escape(_new_conn['password']))
    print("cmd=%s" % cmd)
    code = os.system(cmd)
    if code == 0:
        print("=========> {} load ok.".format(dir))
    else:
        print("=========> {} load fail.".format(dir))


def dump(db, table):
    """ 从旧数据源dump表 """
    global _old_conn
    cmd = './mydumper -P {port} -h {host} -db {db} -table {table} -t 8 -u {user} -p {password} -m doris -d {dir} -vars {vars} -chunk-size {cs}'.format(
        port=_old_conn['port'],
        host=_old_conn['host'],
        db=db,
        table=table,
        user=_old_conn['username'],
        password=escape(_old_conn['password']),
        dir=_old_conn['dir'],
        cs=128,  # 默认chunk size 1个G
        vars='"SET query_timeout=3600;SET exec_mem_limit=20737418240"')
    print("cmd=%s" % cmd)
    code = os.system(cmd)
    if code == 0:
        print("=========> {}.{} dump ok.".format(db, table))
    else:
        print("=========> {}.{} dump fail.".format(db, table))


@click.command()
@click.option('--old_host', type=str)
@click.option('--old_port', type=int)
@click.option('--old_user', type=str)
@click.option('--old_password', type=str)
@click.option('--new_host', type=str)
@click.option('--new_port', type=int)
@click.option('--new_user', type=str)
@click.option('--new_password', type=str)
@click.option('--db', help='target db, will scan all tables.')
@click.option('--dir')
def do(db, old_host, old_port, old_user, old_password, new_host, new_port,
       new_user, new_password, dir):
    global _new_conn, _old_conn
    _old_conn = {
        'host': old_host,
        'port': old_port,
        'username': old_user,
        'password': old_password,
        'dir': dir
    }

    _new_conn = {
        'host': new_host,
        'port': new_port,
        'username': new_user,
        'password': new_password,
        'dir': dir
    }

    print('old_conn: {}'.format(_old_conn))
    print('---------------------------------')
    print('new_conn: {}'.format(_new_conn))

    # 检查差异表
    dumps = []
    for tb in all_tables(db):
        if check(db, tb):
            dumps.append(tb)
    
    if not dumps:
        return

    # dump 差异表
    dump(db, ','.join(dumps))

    # load
    load(_new_conn['dir'])


if __name__ == '__main__':
    do()
