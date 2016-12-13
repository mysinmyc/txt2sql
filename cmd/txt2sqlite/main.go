package main

import (
	"bufio"
	"flag"
	"os"

	"time"

	"github.com/mysinmyc/gocommons/diagnostic"
	"github.com/mysinmyc/txt2sql"
)

func main() {

	vParameterInFile := flag.String("inFile", "", "Input file")
	vParameterOutDb := flag.String("outDb", "", "Output db")
	vParameterDelimiter := flag.String("delimiter", "\t", "Field delimiter")
	vParameterTableName := flag.String("table", "", "Destination table")
	vParameterStringQualifier := flag.String("stringQualifier", "", "String qualifier")
	vParameterThreads := flag.Int("threads", 4, "Threads")
	flag.Parse()

	if *vParameterInFile == "" {
		diagnostic.LogFatal("main", "missing parameter inFile (input file)", nil)
	}

	if *vParameterOutDb == "" {
		diagnostic.LogFatal("main", "missing parameter outDb (output db)", nil)
	}

	if *vParameterTableName == "" {
		diagnostic.LogFatal("main", "missing parameter table (tablename)", nil)
	}

	vInputFile, vInputFileError := os.Open(*vParameterInFile)
	diagnostic.LogFatalIfError(vInputFileError, "main", "failed to open input file %s", *vParameterInFile)
	defer vInputFile.Close()
	vScanner := bufio.NewScanner(vInputFile)

	if !vScanner.Scan() {
		diagnostic.LogFatal("main", "File empty", nil)
	}

	vLoader := txt2sql.NewLoader(txt2sql.LoadingParameters{ConnectionString: *vParameterOutDb, TableName: *vParameterTableName, ColumnDelimiter: *vParameterDelimiter, StringQualifier: *vParameterStringQualifier, BatchSize: 20000})

	vStart := time.Now()
	vLoaded, vDiscarded, vLoadError := vLoader.Load(vInputFile, *vParameterThreads)
	vEnd := time.Now()
	diagnostic.LogFatalIfError(vLoadError, "main", "failed to load data")

	if vDiscarded > 0 {
		diagnostic.LogFatal("main", "Load failed. Too many rows discarded: %d", nil, vDiscarded)
	} else {
		diagnostic.LogInfo("main", "Load succeded. Rows processed %d in %s ", vLoaded, vEnd.Sub(vStart))
	}
}
