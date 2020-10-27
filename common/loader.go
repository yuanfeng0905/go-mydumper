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
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// Files tuple.
type Files struct {
	databases []string
	schemas   []string
	tables    []string
}

var (
	dbSuffix     = "-schema-create.sql"
	schemaSuffix = "-schema.sql"
	tableSuffix  = ".sql"
	csvSuffix    = ".csv"
)

func loadFiles(log *xlog.Log, dir string) *Files {
	files := &Files{}
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Panicf("loader.file.walk.error:%+v", err)
		}

		if !info.IsDir() {
			switch {
			case strings.HasSuffix(path, dbSuffix):
				files.databases = append(files.databases, path)
			case strings.HasSuffix(path, schemaSuffix):
				files.schemas = append(files.schemas, path)
			default:
				if strings.HasSuffix(path, tableSuffix) || strings.HasSuffix(path, csvSuffix) {
					files.tables = append(files.tables, path)
				}
			}
		}
		return nil
	}); err != nil {
		log.Panicf("loader.file.walk.error:%+v", err)
	}
	return files
}

func restoreDatabaseSchema(log *xlog.Log, dbs []string, conn *Connection) {
	for _, db := range dbs {
		base := filepath.Base(db)
		name := strings.TrimSuffix(base, dbSuffix)

		data, err := ReadFile(db)
		AssertNil(err)
		sql := common.BytesToString(data)

		err = conn.Execute(sql)
		AssertNil(err)
		log.Info("restoring.database[%s]", name)
	}
}

func restoreTableSchema(log *xlog.Log, overwrite bool, tables []string, conn *Connection) {
	for _, table := range tables {
		// use
		base := filepath.Base(table)
		name := strings.TrimSuffix(base, schemaSuffix)
		db := strings.Split(name, ".")[0]
		tbl := strings.Split(name, ".")[1]
		name = fmt.Sprintf("`%v`.`%v`", db, tbl)

		log.Info("working.table[%s.%s]", db, tbl)

		err := conn.Execute(fmt.Sprintf("USE `%s`", db))
		AssertNil(err)

		// doris 不支持
		// err = conn.Execute("SET FOREIGN_KEY_CHECKS=0")
		// AssertNil(err)

		data, err := ReadFile(table)
		AssertNil(err)
		query1 := common.BytesToString(data)
		querys := strings.Split(query1, ";\n")
		for _, query := range querys {
			if !strings.HasPrefix(query, "/*") && query != "" {
				if overwrite {
					log.Info("drop(overwrite.is.true).table[%s.%s]", db, tbl)
					dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", name)
					err = conn.Execute(dropQuery)
					AssertNil(err)
				}
				err = conn.Execute(query)
				AssertNil(err)
			}
		}
		log.Info("restoring.schema[%s.%s]", db, tbl)
	}
}

func submitDorisTask(db string, table string, header string, body string, args *Args) error {
	_url := fmt.Sprintf("http://%s/api/%s/%s/_stream_load", args.DorisHttpLoadAddress, db, table)

	req, err := http.NewRequest("POST", _url, strings.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Add("Expect", "100-continue")
	req.Header.Add("Content-Length", string(len(body)))
	req.Header.Add("columns", header)

	cli := http.DefaultClient
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == 307 {
		req.URL, err = url.Parse(resp.Header.Get("Location")) // 重定向到BE节点
		if err != nil {
			return err
		}
		resp, err := cli.Do(req) // retry
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		b, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode == 200 {
			return nil
		}

		return fmt.Errorf("doris response tables[%s.%s], code:%v, body:%v", db, table, resp.StatusCode, b)
	}

	if resp.StatusCode == 200 {
		return nil
	}

	return fmt.Errorf("doris response tables[%s.%s] code:%v, body:%v", db, table, resp.StatusCode, b)
}

func restoreDorisTable(log *xlog.Log, table string, conn *Connection, args *Args) int {
	bytes := 0
	part := "0"
	base := filepath.Base(table)
	name := strings.TrimSuffix(base, tableSuffix)
	splits := strings.Split(name, ".")
	db := splits[0]
	tbl := splits[1]
	if len(splits) > 2 {
		part = splits[2]
	}

	log.Info("restoring.tables[%s.%s].parts[%s].thread[%d]", db, tbl, part, conn.ID)
	err := conn.Execute(fmt.Sprintf("USE `%s`", db))
	AssertNil(err)

	//err = conn.Execute("SET FOREIGN_KEY_CHECKS=0")
	//AssertNil(err)

	data, err := ReadFile(table)
	AssertNil(err)
	query1 := common.BytesToString(data)
	pos := strings.Index(query1, "\n") // 找到第一个换行符
	header := query1[0:pos]            // 第一行是表头
	body := query1[pos+1:]             // 从第二行开始是正文
	bytes = len(query1)

	AssertNil(submitDorisTask(db, tbl, header, body, args))

	log.Info("restoring.tables[%s.%s].parts[%s].thread[%d].done...", db, tbl, part, conn.ID)
	return bytes
}

func restoreTable(log *xlog.Log, table string, conn *Connection) int {
	bytes := 0
	part := "0"
	base := filepath.Base(table)
	name := strings.TrimSuffix(base, tableSuffix)
	splits := strings.Split(name, ".")
	db := splits[0]
	tbl := splits[1]
	if len(splits) > 2 {
		part = splits[2]
	}

	log.Info("restoring.tables[%s.%s].parts[%s].thread[%d]", db, tbl, part, conn.ID)
	err := conn.Execute(fmt.Sprintf("USE `%s`", db))
	AssertNil(err)

	//err = conn.Execute("SET FOREIGN_KEY_CHECKS=0")
	//AssertNil(err)

	data, err := ReadFile(table)
	AssertNil(err)
	query1 := common.BytesToString(data)
	querys := strings.Split(query1, ";\n")
	bytes = len(query1)
	for _, query := range querys {
		if !strings.HasPrefix(query, "/*") && query != "" {
			err = conn.Execute(query)
			AssertNil(err)
		}
	}
	log.Info("restoring.tables[%s.%s].parts[%s].thread[%d].done...", db, tbl, part, conn.ID)
	return bytes
}

// Loader used to start the loader worker.
func Loader(log *xlog.Log, args *Args) {
	pool, err := NewPool(log, args.Threads, args.Address, args.User, args.Password, args.SessionVars)
	AssertNil(err)
	defer pool.Close()

	files := loadFiles(log, args.Outdir)

	// database.
	conn := pool.Get()
	restoreDatabaseSchema(log, files.databases, conn)
	pool.Put(conn)

	// tables.
	conn = pool.Get()
	restoreTableSchema(log, args.OverwriteTables, files.schemas, conn)
	pool.Put(conn)

	// Shuffle the tables
	for i := range files.tables {
		j := rand.Intn(i + 1)
		files.tables[i], files.tables[j] = files.tables[j], files.tables[i]
	}

	var wg sync.WaitGroup
	var bytes uint64
	t := time.Now()
	for _, table := range files.tables {
		conn := pool.Get()
		wg.Add(1)
		go func(conn *Connection, table string) {
			defer func() {
				wg.Done()
				pool.Put(conn)
			}()
			var r int
			if args.Mode == "doris" {
				r = restoreDorisTable(log, table, conn, args)
			} else {
				r = restoreTable(log, table, conn)
			}
			atomic.AddUint64(&bytes, uint64(r))
		}(conn, table)
	}

	tick := time.NewTicker(time.Millisecond * time.Duration(args.IntervalMs))
	defer tick.Stop()
	go func() {
		for range tick.C {
			diff := time.Since(t).Seconds()
			bytes := float64(atomic.LoadUint64(&bytes) / 1024 / 1024)
			rates := bytes / diff
			log.Info("restoring.allbytes[%vMB].time[%.2fsec].rates[%.2fMB/sec]...", bytes, diff, rates)
		}
	}()

	wg.Wait()
	elapsed := time.Since(t).Seconds()
	log.Info("restoring.all.done.cost[%.2fsec].allbytes[%.2fMB].rate[%.2fMB/s]", elapsed, float64(bytes/1024/1024), (float64(bytes/1024/1024) / elapsed))
}
