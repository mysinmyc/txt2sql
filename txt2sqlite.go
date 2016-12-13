package txt2sql

import (
	"bufio"
	"database/sql"
	"os"

	"strings"

	"sync"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mysinmyc/gocommons/concurrent"
	"github.com/mysinmyc/gocommons/diagnostic"
)

type LoadingParameters struct {
	Driver           string
	ConnectionString string
	TableName        string
	ColumnDelimiter  string
	StringQualifier  string
	BatchSize        int
}

type Loader struct {
	LoadingParameters LoadingParameters
	Initialized       bool
	converter         *Converter
	dispatcher        *concurrent.Dispatcher
	workersLocals     map[int]workerLocals
	tableDdl          string
	lock              sync.Mutex
	rowsOk            int64
	rowsKo            int64
}

type workerLocals struct {
	rowsOk int64
	rowsKo int64
	db     *sql.DB
}

func NewLoader(pLoadingParameters LoadingParameters) *Loader {
	vRis := &Loader{LoadingParameters: pLoadingParameters}
	vRis.LoadingParameters.Driver = "sqlite3"
	if vRis.LoadingParameters.ColumnDelimiter == "" {
		vRis.LoadingParameters.ColumnDelimiter = "\t"
	}
	if vRis.LoadingParameters.BatchSize == 0 {
		vRis.LoadingParameters.BatchSize = 10000
	}

	vRis.dispatcher = concurrent.NewDispatcher(vRis.processRow, 10000)
	vRis.dispatcher.WorkerLifeCycleHandlerFunc = vRis.workerLifeCycleFunc
	return vRis
}

func (vSelf *Loader) openConnection() (*sql.DB, error) {

	vRis, vError := sql.Open(vSelf.LoadingParameters.Driver, ":memory:")

	if vError != nil {
		return nil, diagnostic.NewError("failed to open connection ", vError)
	}

	vRis.SetMaxOpenConns(1)
	return vRis, nil
}

func (vSelf *Loader) workerLifeCycleFunc(pDispatcher *concurrent.Dispatcher, pWorkerId int, pEvent concurrent.WorkerLifeCycleEvent, pWorkerLocals concurrent.WorkerLocals) (concurrent.WorkerLocals, error) {

	switch pEvent {
	case concurrent.WorkerLifeCycleEvent_Started:

		vRis := &workerLocals{}

		vDb, vError := vSelf.openConnection()

		if vError != nil {
			return nil, vError
		}
		vRis.db = vDb

		_, vError = vDb.Exec("ATTACH DATABASE ? AS real", vSelf.LoadingParameters.ConnectionString)
		if vError != nil {
			return nil, diagnostic.NewError("Failed to attach inmemory db", vError)
		}

		_, vError = vDb.Exec(strings.Replace(vSelf.tableDdl, "##TABLE##", "worker", -1))
		if vError != nil {
			return nil, diagnostic.NewError("Failed to create temp table", vError)
		}

		return vRis, nil
		break

	case concurrent.WorkerLifeCycleEvent_Stopped:

		defer pWorkerLocals.(*workerLocals).db.Close()
		vError := vSelf.CommitBatch(pWorkerId, pWorkerLocals)

		vSelf.lock.Lock()
		vSelf.rowsOk = vSelf.rowsOk + pWorkerLocals.(*workerLocals).rowsOk
		vSelf.rowsKo += pWorkerLocals.(*workerLocals).rowsKo
		vSelf.lock.Unlock()
		return nil, vError

	}

	return nil, nil
}

func (vSelf *Loader) CommitBatch(pWorkerId int, pWorkerLocals concurrent.WorkerLocals) error {

	var vError error

	vSelf.lock.Lock()

	if vSelf.Initialized == false {
		_, vError = pWorkerLocals.(*workerLocals).db.Exec("create table if not exists real.\"" + vSelf.LoadingParameters.TableName + "\" as select * from worker where 1=2")
		if vError != nil {
			vSelf.lock.Unlock()
			return diagnostic.NewError("insert", vError)
		}
		vSelf.Initialized = true
	}

	_, vError = pWorkerLocals.(*workerLocals).db.Exec("insert into real.\"" + vSelf.LoadingParameters.TableName + "\"  select * from worker")
	if vError != nil {
		vSelf.lock.Unlock()
		return diagnostic.NewError("insert", vError)
	}
	vSelf.lock.Unlock()

	_, vError = pWorkerLocals.(*workerLocals).db.Exec("delete from worker")
	if vError != nil {
		return diagnostic.NewError("Commit batch", vError)
	}

	return nil
}

func (vSelf *Loader) Load(pInputFile *os.File, pWorkers int) (int64, int64, error) {

	diagnostic.LogInfo("LoadFileIntoTable", "Started to load into table %s (batch size %d) ...", vSelf.LoadingParameters.TableName, vSelf.LoadingParameters.BatchSize)

	vScanner := bufio.NewScanner(pInputFile)

	vParser := NewParser(vSelf.LoadingParameters.ColumnDelimiter, vSelf.LoadingParameters.StringQualifier)

	if !vScanner.Scan() {
		return 0, 0, diagnostic.NewError("Input file empty", nil)
	}

	vColumnsParsed, _ := vParser.ParseLine(vScanner.Text())
	vColumnsName := FieldsToStringArray(vColumnsParsed)

	vSqlGenerator := NewSqlGenerator("sqlite", "##TABLE##", vColumnsName)
	vSelf.converter = NewConverter(vParser, vSqlGenerator)
	vSelf.tableDdl = vSqlGenerator.CreateTableDdl()

	vSelf.dispatcher.Start(pWorkers)
	for vScanner.Scan() {
		vCurRow := vScanner.Text()
		vSelf.dispatcher.Enqueue(vCurRow)
	}

	vSelf.dispatcher.WaitForCompletition()

	diagnostic.LogInfo("Loader.Load", "load terminated, rows ok: %d, rows ko %d", vSelf.rowsOk, vSelf.rowsKo)

	return vSelf.rowsOk, vSelf.rowsKo, nil
}

func (vSelf *Loader) processRow(pDispatcher *concurrent.Dispatcher, pWorkerId int, pRow interface{}, pWorkerLocals concurrent.WorkerLocals) error {

	vWorkerLocals := pWorkerLocals.(*workerLocals)
	vInsertError := vSelf.insertRow(pDispatcher, pRow, pWorkerId, pWorkerLocals)

	if vInsertError == nil {

		vWorkerLocals.rowsOk++
		if vWorkerLocals.rowsOk%int64(vSelf.LoadingParameters.BatchSize) == 0 {
			diagnostic.LogInfo("Loader.processRow", "Worker %d Reached %d rows", pWorkerId, vWorkerLocals.rowsOk)
			vCommitError := vSelf.CommitBatch(pWorkerId, pWorkerLocals)
			if vCommitError != nil {
				return diagnostic.NewError("Failed to commit", vCommitError)
			}
		}

	} else {
		vWorkerLocals.rowsKo++
		return vInsertError
	}

	return nil
}

func (vSelf *Loader) insertRow(pDispatcher *concurrent.Dispatcher, pRow interface{}, pWorkerId int, pWorkerLocals concurrent.WorkerLocals) error {

	vCurInsertStatement, vCurInsertStatementError := vSelf.converter.ConvertRow(pRow.(string))

	if vCurInsertStatementError != nil {
		return diagnostic.NewError("Error while processing row %s", vCurInsertStatementError, pRow)
	}

	vCurInsertStatement = strings.Replace(vCurInsertStatement, "##TABLE##", "worker", -1)

	_, vCurInsertError := pWorkerLocals.(*workerLocals).db.Exec(vCurInsertStatement)
	if vCurInsertError != nil {
		return diagnostic.NewError("Error while processing row %s", vCurInsertError, pRow)
	}

	return nil
}
