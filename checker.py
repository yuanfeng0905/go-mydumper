#coding:utf-8

from contextlib import contextmanager
from pymysql.connections import Connection
import click
import os

_new_conn = {
}
_old_conn = {
}


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
            return (db, table)
    


def dump(db, table):
    global _new_conn, _old_conn
    code = os.system(
        './mydumper -P {port} -h {host} -db {db} -table {table} -t 1 -u {user} -p {password} -m doris -d ./sql -vars {vars}'
        .format(port=_new_conn['port'],
                host=_new_conn['host'],
                db=db,
                table=table,
                user=_new_conn['username'],
                password=_new_conn['password']),
                vars='SET query_timeout=3600;SET exec_mem_limit=10737418240')
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
def do(db, old_host, old_port, old_user, old_password, new_host, new_port, new_user, new_password):
    global _new_conn, _old_conn
    _old_conn = {
        'host': old_host,
        'port': old_port,
        'username': old_user,
        'password': old_password
    }

    _new_conn = {
        'host': new_host,
        'port': new_port,
        'username': new_user,
        'password': new_password
    } 

    print('old_conn: {}'.format(_old_conn))
    print('---------------------------------')
    print('new_conn: {}'.format(_new_conn))
    
    dumps = []
    for tb in all_tables(db):
        target = check(db, tb)
        if target:
            dumps.append(target)

    for db, table in dumps:
        dump(db, table)

if __name__ == '__main__':
    do()
