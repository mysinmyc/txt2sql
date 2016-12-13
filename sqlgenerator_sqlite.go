package txt2sql

import (
	"fmt"
)

type SqlLiteSyntaxBuilder struct {
}

func (*SqlLiteSyntaxBuilder) CreateComment(pString string) string {
	return fmt.Sprintf("/* %s */", pString)
}

func (*SqlLiteSyntaxBuilder) GetStatementDelimiter() string {
	return ";"
}

func (*SqlLiteSyntaxBuilder) EscapeName(pName string) string {
	return fmt.Sprintf("\"%s\"", pName)
}

func (*SqlLiteSyntaxBuilder) StringDbType() string {
	return "text"
}
