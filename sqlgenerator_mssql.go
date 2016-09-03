package txt2sql

import (
	"fmt"
)

type MsSqlSyntaxBuilder struct {
}

func (*MsSqlSyntaxBuilder)	CreateComment(pString string) string {
	return fmt.Sprintf("/* %s */",pString)
}

func (*MsSqlSyntaxBuilder) GetStatementDelimiter() string {
	return ";"
}

func (*MsSqlSyntaxBuilder) EscapeName(pName string) string {
		return fmt.Sprintf("[%s]",pName)
} 

func (*MsSqlSyntaxBuilder) StringDbType() string {
		return "VARCHAR(1000)"
} 