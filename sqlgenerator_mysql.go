package txt2sql

import (
	"fmt"
)

type MySqlSyntaxBuilder struct {
}

func (*MySqlSyntaxBuilder)	CreateComment(pString string) string {
	return fmt.Sprintf("/*%s */",pString)
}

func (*MySqlSyntaxBuilder) GetStatementDelimiter() string {
	return ";"
}

func (*MySqlSyntaxBuilder) EscapeName(pName string) string {
		return fmt.Sprintf("`%s`",pName)
} 

func (*MySqlSyntaxBuilder) StringDbType() string {
		return "VARCHAR(1000)"
} 