package txt2sql

import (
	"errors"
	"log"
	"strings"
)

type SyntaxBuilder interface {
	CreateComment(pString string) string
	GetStatementDelimiter() string
	EscapeName(pName string) string
	StringDbType() string
}

type SqlGenerator struct {
	syntaxBuilder   SyntaxBuilder
	columns         []string
	tableName       string
	insertStatement string
}

func getBuilderForDbType(pDbType string) SyntaxBuilder {
	switch pDbType {
	case "sqlite":
		return &SqlLiteSyntaxBuilder{}
	case "oracle":
		return &OracleSyntaxBuilder{}
	case "mysql":
		return &MySqlSyntaxBuilder{}
	case "sqlserver":
		return &MsSqlSyntaxBuilder{}
	default:
		log.Fatal("invalid dbtype:" + pDbType)
		return nil
	}

}

func NewSqlGenerator(pDbType string, pTableName string, pColumns []string) *SqlGenerator {

	vRis := &SqlGenerator{}
	vRis.init(pDbType, pTableName, pColumns)
	return vRis
}

func (self *SqlGenerator) init(pDbType string, pTableName string, pColumns []string) {

	self.syntaxBuilder = getBuilderForDbType(pDbType)
	self.tableName = pTableName
	self.columns = pColumns
	self.insertStatement = "insert into " + self.GetTableName() + " ("
	for vCnt, vCurColumn := range self.columns {
		if vCnt > 0 {
			self.insertStatement += ", "
		}
		self.insertStatement += self.syntaxBuilder.EscapeName(strings.TrimSpace(vCurColumn))
	}
	self.insertStatement += ")"
}

func (self *SqlGenerator) CreateInsertFrom(pFields []Field) (string, error) {

	if len(pFields) != len(self.columns) {
		return "", errors.New("Invalid number of columns")
	}

	vRis := self.insertStatement + " values("

	for vCnt, vCurField := range pFields {

		if vCnt > 0 {
			vRis += ", "
		}
		vRis += "'" + vCurField.value + "'"
	}
	vRis += ")" + self.syntaxBuilder.GetStatementDelimiter()

	return vRis, nil
}

func (self *SqlGenerator) GetTableName() string {
	vRis := ""

	for vCnt, vCurName := range strings.Split(self.tableName, ".") {

		if vCnt > 0 {
			vRis += "."
		}
		vRis += self.syntaxBuilder.EscapeName(vCurName)
	}
	return vRis
}
func (self *SqlGenerator) CreateTableDdl() string {

	vRis := "create table " + self.GetTableName() + " ("
	for vCnt, vCurColumn := range self.columns {

		if vCnt > 0 {
			vRis += ", "
		}
		vRis += self.syntaxBuilder.EscapeName(strings.TrimSpace(vCurColumn)) + " " + self.syntaxBuilder.StringDbType()
	}
	vRis += ")" + self.syntaxBuilder.GetStatementDelimiter()

	return vRis
}

func (self *SqlGenerator) CreateComment(pString string) string {
	return self.syntaxBuilder.CreateComment(pString)
}
