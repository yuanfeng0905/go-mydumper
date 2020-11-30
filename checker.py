#coding:utf-8

from contextlib import contextmanager
from pymysql.connections import Connection
import click
import os

_new_conn = {}
_old_conn = {}


@contextmanager
def get_doris_cur(conn):
    c = Connection(host=conn['host'],
                   port=conn['port'],
                   user=conn['username'],
                   passwd=conn['password'],
                   use_unicode=True,
                   charset='utf8')
    c.autocommit(True)

    cur = c.cursor()
    yield cur
    cur.close()
    c.close()


def all_dbs(db):
    dbs = []
    if db.endswith('*'):
        prefix = db.replace('*', '')
        with get_doris_cur(_old_conn) as cur:
            cur.execute('show databases')
            for l in cur.fetchall():
                if l[0].startswith(prefix) and l[0].find('__mysql__') == -1:
                    dbs.append(l[0])
    else:
        dbs.append(db)
    return dbs


def all_tables(db):
    tbs = []
    with get_doris_cur(_old_conn) as cur:
        cur.execute('use {}'.format(db))
        cur.execute('show tables')
        for l in cur.fetchall():
            tbs.append(l[0])
    return tbs


def check(db, table):
    print("check {}.{}...".format(db, table))
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

        if old_cnt == 0:
            return False

        if old_cnt - new_cnt > 1000 or new_cnt == 0:
            print("=======> need recovery {}.{}".format(db, table))
            return True


def escape(s):
    return s.replace('!', "\!").replace('$', "\$")


def gendir(db):
    dir = './dump_%s_sql' % db
    if not os.path.exists(dir):
        os.mkdir(dir)
    return dir


def load(db, force=False):
    global _new_conn
    dir = gendir(db)
    cmd = './myloader -dp 10.7.51.44:8040,10.7.66.46:8040,10.7.84.112:8040,10.7.187.18:8040 -P {port} -d {dir} -h {host} -m doris -u {user} -p {password} -t 8'.format(
        dir=dir,
        host=_new_conn['host'],
        port=_new_conn['port'],
        user=_new_conn['username'],
        password=escape(_new_conn['password']))
    if force:
        cmd = cmd + ' -o'

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
        dir=gendir(db),
        cs=128,  # 默认chunk size 1个G
        vars='"SET query_timeout=7200;SET exec_mem_limit=20737418240"')
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
@click.option('--table', help='target table')
@click.option('--skip_dump', is_flag=True, help='skip dump diff table.')
@click.option('--skip_load', is_flag=True, help='skip load diff table.')
@click.option('--force', help='force drop table', is_flag=True)
def main(db, table, old_host, old_port, old_user, old_password, new_host, new_port,
         new_user, new_password, skip_dump, skip_load, force):
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

    for _db in all_dbs(db):
        # 检查差异表
        dumps = []
        if table:
            if check(_db, table):
                dumps.append(table)
        else:
            for tb in all_tables(_db):
                if check(_db, tb):
                    dumps.append(tb)

        if not dumps:
            continue

        # dump 差异表
        if not skip_dump:
            dump(_db, ','.join(dumps))

        # load
        if not skip_load:
            load(_db, force=force)


if __name__ == '__main__':
    main()
