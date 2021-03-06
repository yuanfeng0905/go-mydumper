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
	"strings"

	"github.com/yuanfeng0905/go-mydumper/common"

	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	flagOverwriteTables                                                     bool
	flagPort, flagThreads                                                   int
	flagUser, flagPasswd, flagHost, flagDir, flagMode, flagDorisLoadAddress string

	log = xlog.NewStdLog(xlog.Level(xlog.INFO))
)

func init() {
	flag.StringVar(&flagUser, "u", "", "Username with privileges to run the loader")
	flag.StringVar(&flagPasswd, "p", "", "User password")
	flag.StringVar(&flagHost, "h", "", "The host to connect to")
	flag.IntVar(&flagPort, "P", 3306, "TCP/IP port to connect to")
	flag.StringVar(&flagDir, "d", "", "Directory of the dump to import")
	flag.IntVar(&flagThreads, "t", 16, "Number of threads to use")
	flag.BoolVar(&flagOverwriteTables, "o", false, "Drop tables if they already exist")
	flag.StringVar(&flagMode, "m", "", "doris mode for support Doris MPP (default \"mysql\")")
	flag.StringVar(&flagDorisLoadAddress, "dp", "", "doris mode for HTTP Load address (example: \"127.0.0.1:8040,127.0.0.2:8040\")")
}

func usage() {
	fmt.Println("Usage: " + os.Args[0] + " -h [HOST] -P [PORT] -u [USER] -p [PASSWORD] -d [DIR] [-o]")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = func() { usage() }
	flag.Parse()

	if flagHost == "" || flagUser == "" || flagDir == "" {
		usage()
		os.Exit(0)
	}

	args := &common.Args{
		Mode:                 flagMode,
		DorisHttpLoadAddress: strings.Split(flagDorisLoadAddress, ","),
		User:                 flagUser,
		Password:             flagPasswd,
		Address:              fmt.Sprintf("%s:%d", flagHost, flagPort),
		Outdir:               flagDir,
		Threads:              flagThreads,
		IntervalMs:           10 * 1000,
		OverwriteTables:      flagOverwriteTables,
	}

	common.Loader(log, args)
}
