/*
 * go-mydumper
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/yuanfeng0905/go-mydumper/common"

	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	flagUser, flagPasswd, flagHost, flagConfig, flagDB, flagTable, flagOutDir, flagMode, flagVars string
	flagPort, flagThreads, flagChunkSize                                                          *int

	log = xlog.NewStdLog(xlog.Level(xlog.INFO))
)

func init() {
	flag.StringVar(&flagUser, "u", "", "Username with privileges to run the loader")
	flag.StringVar(&flagPasswd, "p", "", "User password")
	flag.StringVar(&flagHost, "h", "", "The host to connect to")
	flagPort = flag.Int("P", 3306, "TCP/IP port to connect to")
	flag.StringVar(&flagConfig, "c", "", "config file")
	flag.StringVar(&flagDB, "db", "", "source db")
	flag.StringVar(&flagTable, "table", "", "source table")
	flag.StringVar(&flagOutDir, "d", "", "Directory of the dump to import")
	flagThreads = flag.Int("t", 16, "Number of threads to use")
	flag.StringVar(&flagMode, "m", "", "doris mode for support Doris MPP (default \"mysql\")")
	flagChunkSize = flag.Int("chunk-size", 128, "default chunk size (MB)")
	flag.StringVar(&flagVars, "vars", "", "variables")

}

func usage() {
	fmt.Println("Usage: " + os.Args[0] + " -c conf/mydumper.ini.sample")
	flag.PrintDefaults()
}

// 解析命令行，覆盖
func recoveryConfig(args *common.Args) {
	if flagHost != "" && flagPort != nil {
		args.Address = fmt.Sprintf("%s:%d", flagHost, *flagPort)
	}
	if flagUser != "" {
		args.User = flagUser
	}
	if flagPasswd != "" {
		args.Password = flagPasswd
	}
	if flagDB != "" {
		args.Database = flagDB
	}
	if flagTable != "" {
		args.Table = flagTable
	}
	if flagThreads != nil {
		args.Threads = *flagThreads
	}
	if flagOutDir != "" {
		args.Outdir = flagOutDir
	}
	if flagMode != "" {
		args.Mode = flagMode
	}
	if flagChunkSize != nil {
		args.ChunksizeInMB = *flagChunkSize
	}
	if flagVars != "" {
		args.SessionVars = flagVars
	}
}

func main() {
	flag.Usage = func() { usage() }
	flag.Parse()

	args, err := common.ParseDumperConfig(flagConfig)
	common.AssertNil(err)

	recoveryConfig(args)

	log.Info("%v", args)

	if _, err := os.Stat(args.Outdir); os.IsNotExist(err) {
		x := os.MkdirAll(args.Outdir, 0777)
		common.AssertNil(x)
	}

	common.Dumper(log, args)
}
