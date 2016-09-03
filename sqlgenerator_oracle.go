package txt2sql

import (
	"fmt"
)

type OracleSyntaxBuilder struct {
}

func (*OracleSyntaxBuilder)	CreateComment(pString string) string {
	return fmt.Sprintf("/* %s */",pString)
}

func (*OracleSyntaxBuilder) GetStatementDelimiter() string {
	return ";"
}

func (*OracleSyntaxBuilder) EscapeName(pName string) string {
		return fmt.Sprintf("\"%s\"",pName)
} 

func (*OracleSyntaxBuilder) StringDbType() string {
		return "VARCHAR2(1000)"
} 