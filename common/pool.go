/*
 * go-mydumper
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

import (
	"strings"
	"sync"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/xlog"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// Pool tuple.
type Pool struct {
	mu    sync.RWMutex
	log   *xlog.Log
	conns chan *Connection
}

// Connection tuple.
type Connection struct {
	ID       int
	client   driver.Conn
	address  string
	user     string
	password string
	vars     string
}

// Execute used to executes the query.
func (conn *Connection) Execute(query string) error {
	return conn.client.Exec(query)
}

// Fetch used to fetch the results.
func (conn *Connection) Fetch(query string) (*sqltypes.Result, error) {
	return conn.client.FetchAll(query, -1)
}

// StreamFetch used to the results with streaming.
func (conn *Connection) StreamFetch(query string) (driver.Rows, error) {
	return conn.client.Query(query)
}

// NewPool creates the new pool.
func NewPool(log *xlog.Log, cap int, address string, user string, password string, vars string) (*Pool, error) {
	conns := make(chan *Connection, cap)
	for i := 0; i < cap; i++ {
		client, err := driver.NewConn(user, password, address, "", "utf8")
		if err != nil {
			return nil, err
		}
		conn := &Connection{ID: i, client: client, address: address, user: user, password: password, vars: vars}
		if vars != "" {
			varSp := strings.Split(vars, ";")
			for _, v := range varSp {
				if err := conn.Execute(v); err != nil {
					return nil, err
				}
			}
		}
		conns <- conn
	}

	return &Pool{
		log:   log,
		conns: conns,
	}, nil
}

// Get used to get one connection from the pool.
func (p *Pool) Get() *Connection {
	conns := p.getConns()
	if conns == nil {
		return nil
	}
	conn := <-conns
	// 检查链接是否有效
	if err := conn.client.Ping(); err != nil {
		p.log.Warning("current conn[%d].client is invalid, renew...", conn.ID)
		if !conn.client.Closed() {
			conn.client.Close()
		}
		// 生成新的client
		client, err := driver.NewConn(conn.user, conn.password, conn.address, "", "utf8")
		if err != nil {
			panic(err)
		}
		conn.client = client // update
		if conn.vars != "" {
			varSp := strings.Split(conn.vars, ";")
			for _, v := range varSp {
				if err := conn.Execute(v); err != nil {
					panic(err)
				}
			}
		}
	}

	return conn
}

// Put used to put one connection to the pool.
func (p *Pool) Put(conn *Connection) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.conns == nil {
		return
	}
	p.conns <- conn
}

// Close used to close the pool and the connections.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	close(p.conns)
	for conn := range p.conns {
		conn.client.Close()
	}
	p.conns = nil
}

func (p *Pool) getConns() chan *Connection {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.conns
}
