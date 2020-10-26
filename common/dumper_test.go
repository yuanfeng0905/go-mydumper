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
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestDumper(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	fakedbs := driver.NewTestHandler(log)
	server, err := driver.MockMysqlServer(log, fakedbs)
	assert.Nil(t, err)
	defer server.Close()
	address := server.Addr()

	selectResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "namei1",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "null",
				Type: querypb.Type_NULL_TYPE,
			},
			{
				Name: "decimal",
				Type: querypb.Type_DECIMAL,
			},
			{
				Name: "datetime",
				Type: querypb.Type_DATETIME,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 201710; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("11\"xx\"")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
			sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, nil),
			sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("210.01")),
			sqltypes.NULL,
		}
		selectResult.Rows = append(selectResult.Rows, row)
	}

	schemaResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Create Table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("CREATE TABLE `t1-05-11` (`a` int(11) DEFAULT NULL,`b` varchar(100) DEFAULT NULL) ENGINE=InnoDB")),
			},
		}}

	tablesResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Tables_in_test",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1-05-11")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t2-05-11")),
			},
		}}

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show create table .*", schemaResult)
		fakedbs.AddQueryPattern("show tables from .*", tablesResult)
		fakedbs.AddQueryPattern("select .*", selectResult)
		fakedbs.AddQueryPattern("set .*", &sqltypes.Result{})
	}

	args := &Args{
		Database:      "test",
		Outdir:        "/tmp/dumpertest",
		User:          "mock",
		Password:      "mock",
		Address:       address,
		ChunksizeInMB: 1,
		Threads:       16,
		StmtSize:      10000,
		IntervalMs:    500,
		SessionVars:   "SET @@radon_streaming_fetch='ON', @@xx=1",
	}

	os.RemoveAll(args.Outdir)
	if _, err := os.Stat(args.Outdir); os.IsNotExist(err) {
		x := os.MkdirAll(args.Outdir, 0777)
		AssertNil(x)
	}

	// Dumper.
	{
		Dumper(log, args)
	}
	dat, err := ioutil.ReadFile(args.Outdir + "/test.t1-05-11.00001.sql")
	assert.Nil(t, err)
	want := strings.Contains(string(dat), `(11,"11\"xx\"","",NULL,210.01,NULL)`)
	assert.True(t, want)
}

func TestDumperAll(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	fakedbs := driver.NewTestHandler(log)
	server, err := driver.MockMysqlServer(log, fakedbs)
	assert.Nil(t, err)
	defer server.Close()
	address := server.Addr()

	selectResult1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "namei1",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "null",
				Type: querypb.Type_NULL_TYPE,
			},
			{
				Name: "decimal",
				Type: querypb.Type_DECIMAL,
			},
			{
				Name: "datetime",
				Type: querypb.Type_DATETIME,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 201710; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("11\"xx\"")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
			sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, nil),
			sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("210.01")),
			sqltypes.NULL,
		}
		selectResult1.Rows = append(selectResult1.Rows, row)
	}

	selectResult2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 201710; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1337")),
		}
		selectResult2.Rows = append(selectResult2.Rows, row)
	}

	schemaResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Create Table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("CREATE TABLE `t1-05-11` (`a` int(11) DEFAULT NULL,`b` varchar(100) DEFAULT NULL) ENGINE=InnoDB")),
			},
		}}

	tablesResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Tables_in_test",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1-05-11")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t2-05-11")),
			},
		}}

	databasesResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Databases_in_database",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test1")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test2")),
			},
		}}

	// fakedbs.
	{
		fakedbs.AddQueryPattern("show databases", databasesResult)
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show create table .*", schemaResult)
		fakedbs.AddQueryPattern("show tables from .*", tablesResult)
		fakedbs.AddQueryPattern("select .* from `test1`.*", selectResult1)
		fakedbs.AddQueryPattern("select .* from `test2`.*", selectResult2)
		fakedbs.AddQueryPattern("set .*", &sqltypes.Result{})
	}

	args := &Args{
		Outdir:        "/tmp/dumpertest",
		User:          "mock",
		Password:      "mock",
		Address:       address,
		ChunksizeInMB: 1,
		Threads:       16,
		StmtSize:      10000,
		IntervalMs:    500,
		SessionVars:   "SET @@radon_streaming_fetch='ON', @@xx=1",
	}

	os.RemoveAll(args.Outdir)
	if _, err := os.Stat(args.Outdir); os.IsNotExist(err) {
		x := os.MkdirAll(args.Outdir, 0777)
		AssertNil(x)
	}

	// Dumper.
	{
		Dumper(log, args)
	}
	dat_test1, err_test1 := ioutil.ReadFile(args.Outdir + "/test1.t1-05-11.00001.sql")
	assert.Nil(t, err_test1)
	want_test1 := strings.Contains(string(dat_test1), `(11,"11\"xx\"","",NULL,210.01,NULL)`)
	assert.True(t, want_test1)
	dat_test2, err_test2 := ioutil.ReadFile(args.Outdir + "/test2.t1-05-11.00001.sql")
	assert.Nil(t, err_test2)
	want_test2 := strings.Contains(string(dat_test2), `(1337)`)
	assert.True(t, want_test2)
}

func TestDumperMultiple(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	fakedbs := driver.NewTestHandler(log)
	server, err := driver.MockMysqlServer(log, fakedbs)
	assert.Nil(t, err)
	defer server.Close()
	address := server.Addr()

	selectResult1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "namei1",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "null",
				Type: querypb.Type_NULL_TYPE,
			},
			{
				Name: "decimal",
				Type: querypb.Type_DECIMAL,
			},
			{
				Name: "datetime",
				Type: querypb.Type_DATETIME,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 201710; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("11\"xx\"")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
			sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, nil),
			sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("210.01")),
			sqltypes.NULL,
		}
		selectResult1.Rows = append(selectResult1.Rows, row)
	}

	selectResult2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 201710; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1337")),
		}
		selectResult2.Rows = append(selectResult2.Rows, row)
	}

	schemaResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Create Table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("CREATE TABLE `t1-05-11` (`a` int(11) DEFAULT NULL,`b` varchar(100) DEFAULT NULL) ENGINE=InnoDB")),
			},
		}}

	tablesResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Tables_in_test",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1-05-11")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t2-05-11")),
			},
		}}

	databasesResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Databases_in_database",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test1")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test2")),
			},
		}}

	// fakedbs.
	{
		fakedbs.AddQueryPattern("show databases", databasesResult)
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show create table .*", schemaResult)
		fakedbs.AddQueryPattern("show tables from .*", tablesResult)
		fakedbs.AddQueryPattern("select .* from `test1`.*", selectResult1)
		fakedbs.AddQueryPattern("select .* from `test2`.*", selectResult2)
		fakedbs.AddQueryPattern("set .*", &sqltypes.Result{})
	}

	args := &Args{
		Database:      "test1,test2",
		Outdir:        "/tmp/dumpertest",
		User:          "mock",
		Password:      "mock",
		Address:       address,
		ChunksizeInMB: 1,
		Threads:       16,
		StmtSize:      10000,
		IntervalMs:    500,
		SessionVars:   "SET @@radon_streaming_fetch='ON', @@xx=1",
	}

	os.RemoveAll(args.Outdir)
	if _, err := os.Stat(args.Outdir); os.IsNotExist(err) {
		x := os.MkdirAll(args.Outdir, 0777)
		AssertNil(x)
	}

	// Dumper.
	{
		Dumper(log, args)
	}
	dat_test1, err_test1 := ioutil.ReadFile(args.Outdir + "/test1.t1-05-11.00001.sql")
	assert.Nil(t, err_test1)
	want_test1 := strings.Contains(string(dat_test1), `(11,"11\"xx\"","",NULL,210.01,NULL)`)
	assert.True(t, want_test1)
	dat_test2, err_test2 := ioutil.ReadFile(args.Outdir + "/test2.t1-05-11.00001.sql")
	assert.Nil(t, err_test2)
	want_test2 := strings.Contains(string(dat_test2), `(1337)`)
	assert.True(t, want_test2)
}

func TestDumperSimpleRegexp(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	fakedbs := driver.NewTestHandler(log)
	server, err := driver.MockMysqlServer(log, fakedbs)
	assert.Nil(t, err)
	defer server.Close()
	address := server.Addr()

	selectResult1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "namei1",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "null",
				Type: querypb.Type_NULL_TYPE,
			},
			{
				Name: "decimal",
				Type: querypb.Type_DECIMAL,
			},
			{
				Name: "datetime",
				Type: querypb.Type_DATETIME,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 201710; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("11\"xx\"")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
			sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, nil),
			sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("210.01")),
			sqltypes.NULL,
		}
		selectResult1.Rows = append(selectResult1.Rows, row)
	}

	selectResult2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 201710; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1337")),
		}
		selectResult2.Rows = append(selectResult2.Rows, row)
	}

	schemaResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Create Table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("CREATE TABLE `t1-05-11` (`a` int(11) DEFAULT NULL,`b` varchar(100) DEFAULT NULL) ENGINE=InnoDB")),
			},
		}}

	tablesResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Tables_in_test",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1-05-11")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t2-05-11")),
			},
		}}

	databasesResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Databases_in_database",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test1")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test2")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test3")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test4")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test5")),
			},
		}}

	// fakedbs.
	{
		fakedbs.AddQueryPattern("show databases", databasesResult)
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show create table .*", schemaResult)
		fakedbs.AddQueryPattern("show tables from .*", tablesResult)
		fakedbs.AddQueryPattern("select .* from `test1`.*", selectResult1)
		fakedbs.AddQueryPattern("select .* from `test2`.*", selectResult2)
		fakedbs.AddQueryPattern("set .*", &sqltypes.Result{})
	}

	args := &Args{
		DatabaseRegexp: "(test1|test2)",
		Outdir:         "/tmp/dumpertest",
		User:           "mock",
		Password:       "mock",
		Address:        address,
		ChunksizeInMB:  1,
		Threads:        16,
		StmtSize:       10000,
		IntervalMs:     500,
		SessionVars:    "SET @@radon_streaming_fetch='ON', @@xx=1",
	}

	os.RemoveAll(args.Outdir)
	if _, err := os.Stat(args.Outdir); os.IsNotExist(err) {
		x := os.MkdirAll(args.Outdir, 0777)
		AssertNil(x)
	}

	// Dumper.
	{
		Dumper(log, args)
	}
	dat_test1, err_test1 := ioutil.ReadFile(args.Outdir + "/test1.t1-05-11.00001.sql")
	assert.Nil(t, err_test1)
	want_test1 := strings.Contains(string(dat_test1), `(11,"11\"xx\"","",NULL,210.01,NULL)`)
	assert.True(t, want_test1)
	dat_test2, err_test2 := ioutil.ReadFile(args.Outdir + "/test2.t1-05-11.00001.sql")
	assert.Nil(t, err_test2)
	want_test2 := strings.Contains(string(dat_test2), `(1337)`)
	assert.True(t, want_test2)
}

func TestDumperComplexRegexp(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	fakedbs := driver.NewTestHandler(log)
	server, err := driver.MockMysqlServer(log, fakedbs)
	assert.Nil(t, err)
	defer server.Close()
	address := server.Addr()

	selectResult1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "namei1",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "null",
				Type: querypb.Type_NULL_TYPE,
			},
			{
				Name: "decimal",
				Type: querypb.Type_DECIMAL,
			},
			{
				Name: "datetime",
				Type: querypb.Type_DATETIME,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 201710; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("11\"xx\"")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
			sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, nil),
			sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("210.01")),
			sqltypes.NULL,
		}
		selectResult1.Rows = append(selectResult1.Rows, row)
	}

	selectResult2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 201710; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1337")),
		}
		selectResult2.Rows = append(selectResult2.Rows, row)
	}

	schemaResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Create Table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("CREATE TABLE `t1-05-11` (`a` int(11) DEFAULT NULL,`b` varchar(100) DEFAULT NULL) ENGINE=InnoDB")),
			},
		}}

	tablesResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Tables_in_test",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1-05-11")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t2-05-11")),
			},
		}}

	databasesResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Databases_in_database",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test1")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test2")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("foo1")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("bar2")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test5")),
			},
		}}

	// fakedbs.
	{
		fakedbs.AddQueryPattern("show databases", databasesResult)
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show create table .*", schemaResult)
		fakedbs.AddQueryPattern("show tables from .*", tablesResult)
		fakedbs.AddQueryPattern("select .* from `test1`.*", selectResult1)
		fakedbs.AddQueryPattern("select .* from `test2`.*", selectResult2)
		fakedbs.AddQueryPattern("set .*", &sqltypes.Result{})
	}

	args := &Args{
		DatabaseRegexp: "^[ets]+?[0-2]$",
		Outdir:         "/tmp/dumpertest",
		User:           "mock",
		Password:       "mock",
		Address:        address,
		ChunksizeInMB:  1,
		Threads:        16,
		StmtSize:       10000,
		IntervalMs:     500,
		SessionVars:    "SET @@radon_streaming_fetch='ON', @@xx=1",
	}

	os.RemoveAll(args.Outdir)
	if _, err := os.Stat(args.Outdir); os.IsNotExist(err) {
		x := os.MkdirAll(args.Outdir, 0777)
		AssertNil(x)
	}

	// Dumper.
	{
		Dumper(log, args)
	}
	dat_test1, err_test1 := ioutil.ReadFile(args.Outdir + "/test1.t1-05-11.00001.sql")
	assert.Nil(t, err_test1)
	want_test1 := strings.Contains(string(dat_test1), `(11,"11\"xx\"","",NULL,210.01,NULL)`)
	assert.True(t, want_test1)
	dat_test2, err_test2 := ioutil.ReadFile(args.Outdir + "/test2.t1-05-11.00001.sql")
	assert.Nil(t, err_test2)
	want_test2 := strings.Contains(string(dat_test2), `(1337)`)
	assert.True(t, want_test2)
}

func TestDumperInvertMatch(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	fakedbs := driver.NewTestHandler(log)
	server, err := driver.MockMysqlServer(log, fakedbs)
	assert.Nil(t, err)
	defer server.Close()
	address := server.Addr()

	selectResult1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "namei1",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "null",
				Type: querypb.Type_NULL_TYPE,
			},
			{
				Name: "decimal",
				Type: querypb.Type_DECIMAL,
			},
			{
				Name: "datetime",
				Type: querypb.Type_DATETIME,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 201710; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("11\"xx\"")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
			sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, nil),
			sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("210.01")),
			sqltypes.NULL,
		}
		selectResult1.Rows = append(selectResult1.Rows, row)
	}

	selectResult2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 201710; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1337")),
		}
		selectResult2.Rows = append(selectResult2.Rows, row)
	}

	schemaResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Create Table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("CREATE TABLE `t1-05-11` (`a` int(11) DEFAULT NULL,`b` varchar(100) DEFAULT NULL) ENGINE=InnoDB")),
			},
		}}

	tablesResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Tables_in_test",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1-05-11")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t2-05-11")),
			},
		}}

	databasesResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Databases_in_database",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test1")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test2")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("mysql")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("sys")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("information_schema")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("performance_schema")),
			},
		}}

	// fakedbs.
	{
		fakedbs.AddQueryPattern("show databases", databasesResult)
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show create table .*", schemaResult)
		fakedbs.AddQueryPattern("show tables from .*", tablesResult)
		fakedbs.AddQueryPattern("select .* from `test1`.*", selectResult1)
		fakedbs.AddQueryPattern("select .* from `test2`.*", selectResult2)
		fakedbs.AddQueryPattern("set .*", &sqltypes.Result{})
	}

	args := &Args{
		DatabaseRegexp:       "^(mysql|sys|information_schema|performance_schema)$",
		DatabaseInvertRegexp: true,
		Outdir:               "/tmp/dumpertest",
		User:                 "mock",
		Password:             "mock",
		Address:              address,
		ChunksizeInMB:        1,
		Threads:              16,
		StmtSize:             10000,
		IntervalMs:           500,
		SessionVars:          "SET @@radon_streaming_fetch='ON', @@xx=1",
	}

	os.RemoveAll(args.Outdir)
	if _, err := os.Stat(args.Outdir); os.IsNotExist(err) {
		x := os.MkdirAll(args.Outdir, 0777)
		AssertNil(x)
	}

	// Dumper.
	{
		Dumper(log, args)
	}
	dat_test1, err_test1 := ioutil.ReadFile(args.Outdir + "/test1.t1-05-11.00001.sql")
	assert.Nil(t, err_test1)
	want_test1 := strings.Contains(string(dat_test1), `(11,"11\"xx\"","",NULL,210.01,NULL)`)
	assert.True(t, want_test1)
	dat_test2, err_test2 := ioutil.ReadFile(args.Outdir + "/test2.t1-05-11.00001.sql")
	assert.Nil(t, err_test2)
	want_test2 := strings.Contains(string(dat_test2), `(1337)`)
	assert.True(t, want_test2)
}
