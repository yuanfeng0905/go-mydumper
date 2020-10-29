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
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func writeMetaData(args *Args) {
	file := fmt.Sprintf("%s/metadata", args.Outdir)
	WriteFile(file, "")
}

func dumpDatabaseSchema(log *xlog.Log, conn *Connection, args *Args, database string) {
	err := conn.Execute(fmt.Sprintf("USE `%s`", database))
	AssertNil(err)

	schema := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", database)
	file := fmt.Sprintf("%s/%s-schema-create.sql", args.Outdir, database)
	WriteFile(file, schema)
	log.Info("dumping.database[%s].schema...", database)
}

func dumpTableSchema(log *xlog.Log, conn *Connection, args *Args, database string, table string) error {
	qr, err := conn.Fetch(fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", database, table))
	if err != nil {
		return err
	}
	schema := qr.Rows[0][1].String() + ";\n"

	// doris模式下，需要特殊处理聚合模式的表
	if args.Mode == "doris" {
		if strings.Index(schema, "UNIQUE KEY") != -1 {
			schema = strings.ReplaceAll(schema, "REPLACE", "") // FIXME
		}
		if strings.Index(schema, "varchar(255)") != -1 {
			schema = strings.ReplaceAll(schema, "varchar(255)", "varchar(512)") // 扩容
		}
	}

	file := fmt.Sprintf("%s/%s.%s-schema.sql", args.Outdir, database, table)
	if err := WriteFile(file, schema); err != nil {
		return err
	}
	log.Info("dumping.table[%s.%s].schema...", database, table)
	return nil
}

// doris 表导出为csv格式
func dumpDorisTable(log *xlog.Log, conn *Connection, args *Args, database string, table string) {
	var allBytes uint64
	var allRows uint64
	var where string
	var extFields []string

	fields := make([]string, 0, 16)
	{
		cursor, err := conn.StreamFetch(fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 1", database, table))
		AssertNil(err)

		fs := cursor.Fields()
		for _, f := range fs {
			log.Debug("dump -- %#v, %s, %s", args.Filters, table, f.Name)
			if _, ok := args.Filters[table][f.Name]; ok {
				continue
			}

			fields = append(fields, fmt.Sprintf("`%s`", f.Name))
			replacement, ok := args.Selects[table][f.Name]
			if ok {
				extFields = append(extFields, fmt.Sprintf("%s AS `%s`", replacement, f.Name))
			} else {
				extFields = append(extFields, fmt.Sprintf("`%s`", f.Name))
			}
		}
		err = cursor.Close()
		AssertNil(err)
	}

	if v, ok := args.Wheres[table]; ok {
		where = fmt.Sprintf(" WHERE %v", v)
	}

	cursor, err := conn.StreamFetch(fmt.Sprintf("SELECT %s FROM `%s`.`%s` %s", strings.Join(extFields, ", "), database, table, where))
	AssertNil(err)

	fileNo := 1
	chunkbytes := 0
	rows := make([]string, 0, 256)
	inserts := make([]string, 0, 256)
	for cursor.Next() {
		row, err := cursor.RowValues()
		AssertNil(err)

		values := make([]string, 0, 16)
		for _, v := range row {
			if v.Raw() == nil {
				values = append(values, "\\N") // doris null值特殊处理
			} else {
				str := v.String()
				switch {
				case v.IsSigned(), v.IsUnsigned(), v.IsFloat(), v.IsIntegral(), v.Type() == querypb.Type_DECIMAL:
					values = append(values, str)
				case v.IsTemporal(): // 兼容doris模式下，日期/时间对象不编码为带引号的字符串
					values = append(values, str)
				default:
					val := fmt.Sprintf("%s", EscapeBytes(v.Raw()))
					val = strings.ReplaceAll(val, "\t", "")
					val = strings.ReplaceAll(val, "\n", "")

					rVal := []rune(val)
					if len(rVal) > 512 {
						rVal = rVal[:512] // 先按512个字符截取
						for len(string(rVal)) > 512 {
							rVal = rVal[:len(rVal)-1] // 按字符缩进
						}
					}
					values = append(values, string(rVal))
				}
			}
		}
		r := strings.Join(values, "\t") // CSV 格式，\t分隔
		rows = append(rows, r)

		allRows++
		chunkbytes += len(r)
		allBytes += uint64(len(r))
		atomic.AddUint64(&args.Allbytes, uint64(len(r)))
		atomic.AddUint64(&args.Allrows, 1)

		if (chunkbytes / 1024 / 1024) >= args.ChunksizeInMB {
			inserts = append(inserts, strings.Join(fields, ","), strings.Join(rows, "\n")) // 文件首行是csv头
			query := strings.Join(inserts, "\n")                                           // 换行
			file := fmt.Sprintf("%s/%s.%s.%05d.csv", args.Outdir, database, table, fileNo)
			WriteFile(file, query)

			log.Info("dumping.table[%s.%s].rows[%v].bytes[%vMB].part[%v].thread[%d]", database, table, allRows, (allBytes / 1024 / 1024), fileNo, conn.ID)
			rows = rows[:0]
			inserts = inserts[:0] // clear
			chunkbytes = 0
			fileNo++
		}
	}
	if chunkbytes > 0 {
		if len(rows) > 0 {
			inserts = append(inserts, strings.Join(fields, ","), strings.Join(rows, "\n"))
		}

		query := strings.Join(inserts, "\n")
		file := fmt.Sprintf("%s/%s.%s.%05d.csv", args.Outdir, database, table, fileNo)
		WriteFile(file, query)
	}
	AssertNil(cursor.Close())

	log.Info("dumping.table[%s.%s].done.allrows[%v].allbytes[%vMB].thread[%d]...", database, table, allRows, (allBytes / 1024 / 1024), conn.ID)
}

func dumpTable(log *xlog.Log, conn *Connection, args *Args, database string, table string) {
	var allBytes uint64
	var allRows uint64
	var where string
	var extFields []string

	fields := make([]string, 0, 16)
	{
		cursor, err := conn.StreamFetch(fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 1", database, table))
		AssertNil(err)

		fs := cursor.Fields()
		for _, f := range fs {
			log.Debug("dump -- %#v, %s, %s", args.Filters, table, f.Name)
			if _, ok := args.Filters[table][f.Name]; ok {
				continue
			}

			fields = append(fields, fmt.Sprintf("`%s`", f.Name))
			replacement, ok := args.Selects[table][f.Name]
			if ok {
				extFields = append(extFields, fmt.Sprintf("%s AS `%s`", replacement, f.Name))
			} else {
				extFields = append(extFields, fmt.Sprintf("`%s`", f.Name))
			}
		}
		err = cursor.Close()
		AssertNil(err)
	}

	if v, ok := args.Wheres[table]; ok {
		where = fmt.Sprintf(" WHERE %v", v)
	}

	cursor, err := conn.StreamFetch(fmt.Sprintf("SELECT %s FROM `%s`.`%s` %s", strings.Join(extFields, ", "), database, table, where))
	AssertNil(err)

	fileNo := 1
	stmtsize := 0
	chunkbytes := 0
	rows := make([]string, 0, 256)
	inserts := make([]string, 0, 256)
	for cursor.Next() {
		row, err := cursor.RowValues()
		AssertNil(err)

		values := make([]string, 0, 16)
		for _, v := range row {
			if v.Raw() == nil {
				values = append(values, "NULL")
			} else {
				str := v.String()
				switch {
				case v.IsSigned(), v.IsUnsigned(), v.IsFloat(), v.IsIntegral(), v.Type() == querypb.Type_DECIMAL:
					values = append(values, str)
				default:
					values = append(values, fmt.Sprintf("\"%s\"", EscapeBytes(v.Raw())))
				}
			}
		}
		r := "(" + strings.Join(values, ",") + ")"
		rows = append(rows, r)

		allRows++
		stmtsize += len(r)
		chunkbytes += len(r)
		allBytes += uint64(len(r))
		atomic.AddUint64(&args.Allbytes, uint64(len(r)))
		atomic.AddUint64(&args.Allrows, 1)

		if stmtsize >= args.StmtSize {
			insertone := fmt.Sprintf("INSERT INTO `%s`(%s) VALUES\n%s", table, strings.Join(fields, ","), strings.Join(rows, ",\n"))
			inserts = append(inserts, insertone)
			rows = rows[:0]
			stmtsize = 0
		}

		if (chunkbytes / 1024 / 1024) >= args.ChunksizeInMB {
			query := strings.Join(inserts, ";\n") + ";\n"
			file := fmt.Sprintf("%s/%s.%s.%05d.sql", args.Outdir, database, table, fileNo)
			WriteFile(file, query)

			log.Info("dumping.table[%s.%s].rows[%v].bytes[%vMB].part[%v].thread[%d]", database, table, allRows, (allBytes / 1024 / 1024), fileNo, conn.ID)
			inserts = inserts[:0]
			chunkbytes = 0
			fileNo++
		}
	}
	if chunkbytes > 0 {
		if len(rows) > 0 {
			insertone := fmt.Sprintf("INSERT INTO `%s`(%s) VALUES\n%s", table, strings.Join(fields, ","), strings.Join(rows, ",\n"))
			inserts = append(inserts, insertone)
		}

		query := strings.Join(inserts, ";\n") + ";\n"
		file := fmt.Sprintf("%s/%s.%s.%05d.sql", args.Outdir, database, table, fileNo)
		WriteFile(file, query)
	}
	err = cursor.Close()
	AssertNil(err)

	log.Info("dumping.table[%s.%s].done.allrows[%v].allbytes[%vMB].thread[%d]...", database, table, allRows, (allBytes / 1024 / 1024), conn.ID)
}

func allTables(log *xlog.Log, conn *Connection, database string) []string {
	qr, err := conn.Fetch(fmt.Sprintf("SHOW TABLES FROM `%s`", database))
	AssertNil(err)

	tables := make([]string, 0, 128)
	for _, t := range qr.Rows {
		tables = append(tables, t[0].String())
	}
	return tables
}

func allDatabases(log *xlog.Log, conn *Connection) []string {
	qr, err := conn.Fetch("SHOW DATABASES")
	AssertNil(err)

	databases := make([]string, 0, 128)
	for _, t := range qr.Rows {
		databases = append(databases, t[0].String())
	}
	return databases
}

func filterDatabases(log *xlog.Log, conn *Connection, filter *regexp.Regexp, invert bool) []string {
	qr, err := conn.Fetch("SHOW DATABASES")
	AssertNil(err)

	databases := make([]string, 0, 128)
	for _, t := range qr.Rows {
		if (!invert && filter.MatchString(t[0].String())) || (invert && !filter.MatchString(t[0].String())) {
			databases = append(databases, t[0].String())
		}
	}
	return databases
}

func filterDorisTable(log *xlog.Log, conn *Connection, database string, tables []string) []string {
	tbs := []string{}
	inTables := "table_name in ('" + strings.Join(tables, "','") + "')"
	qr, err := conn.Fetch(fmt.Sprintf("select table_name from information_schema.tables where %s and table_schema='%s' and engine in ('Doris','InnoDB')", inTables, database))
	AssertNil(err)

	for _, t := range qr.Rows {
		tbs = append(tbs, t[0].String())
	}

	return tbs
}

// Dumper used to start the dumper worker.
func Dumper(log *xlog.Log, args *Args) {
	pool, err := NewPool(log, args.Threads, args.Address, args.User, args.Password, args.SessionVars)
	AssertNil(err)
	defer pool.Close()

	// Meta data.
	writeMetaData(args)

	// database.
	var wg sync.WaitGroup
	conn := pool.Get()
	var databases []string
	t := time.Now()

	if args.DatabaseRegexp != "" {
		r := regexp.MustCompile(args.DatabaseRegexp)
		databases = filterDatabases(log, conn, r, args.DatabaseInvertRegexp)
	} else {
		if args.Database != "" {
			databases = strings.Split(args.Database, ",")
		} else {
			databases = allDatabases(log, conn)
		}
	}
	for _, database := range databases {
		dumpDatabaseSchema(log, conn, args, database)
	}

	// tables.
	tables := make([][]string, len(databases))
	for i, database := range databases {
		if args.Table != "" {
			tables[i] = strings.Split(args.Table, ",")
		} else {
			tables[i] = allTables(log, conn, database)
		}

		// doris 模式下，需要过滤掉特殊表，只dump doris 引擎的表
		if args.Mode == "doris" {
			tables[i] = filterDorisTable(log, conn, database, tables[i])
		}
	}
	pool.Put(conn)

	tick := time.NewTicker(time.Millisecond * time.Duration(args.IntervalMs))
	defer tick.Stop()
	go func() {
		for range tick.C {
			diff := time.Since(t).Seconds()
			allbytesMB := float64(atomic.LoadUint64(&args.Allbytes) / 1024 / 1024)
			allrows := atomic.LoadUint64(&args.Allrows)
			rates := allbytesMB / diff
			log.Info("dumping.allbytes[%vMB].allrows[%v].time[%.2fsec].rates[%.2fMB/sec]...", allbytesMB, allrows, diff, rates)
		}
	}()

	for i, database := range databases {
		for _, table := range tables[i] {
			conn := pool.Get()
			wg.Add(1)

			go func(conn *Connection, database string, table string) {
				defer func() {
					// if err := recover(); err != nil {
					// 	// 线程奔溃，先记录到错误日志，再手动分析
					// 	log.Error("dumping.table[%s.%s] error:%v", database, table, err)
					// }
					wg.Done()
					pool.Put(conn)
				}()

				if err := dumpTableSchema(log, conn, args, database, table); err != nil {
					log.Error("dumping.table.schema[%s.%s] error:%v", database, table, err)
					return
				}

				log.Info("dumping.table[%s.%s].datas.thread[%d]...", database, table, conn.ID)
				if args.Mode == "doris" {
					dumpDorisTable(log, conn, args, database, table)
				} else {
					dumpTable(log, conn, args, database, table)
				}
				log.Info("dumping.table[%s.%s].datas.thread[%d].done...", database, table, conn.ID)
			}(conn, database, table)
		}
	}

	wg.Wait()
	elapsed := time.Since(t).Seconds()
	log.Info("dumping.all.done.cost[%.2fsec].allrows[%v].allbytes[%v].rate[%.2fMB/s]", elapsed, args.Allrows, args.Allbytes, (float64(args.Allbytes/1024/1024) / elapsed))
}
