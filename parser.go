package txt2sql 

import (
	"strings"
)



type Parser struct {
	delimiter string
	stringQualifier string
}


const (
	TYPE_VARCHAR = iota
	TYPE_NUMBER
)

type Field struct {
	value string
	valueType int 
}



func NewParser(pDelimiter string, pStringQualifier string) *Parser {
	return &Parser { delimiter: pDelimiter, stringQualifier:pStringQualifier }
}



func (self *Parser) ParseLine(pLine string) ([]Field, error) {
	
	vLineSplitted:=strings.Split(pLine,self.delimiter) 
	
	var vRis = make([]Field,len(vLineSplitted))

	for vCnt,vCurField := range vLineSplitted {
		
		if self.stringQualifier!="" && strings.HasPrefix(vCurField,self.stringQualifier) {
			vCurField=vCurField[len( self.stringQualifier):]
		}
			
		if self.stringQualifier!="" && strings.HasSuffix(vCurField,self.stringQualifier) {
			vCurField=vCurField[0:len(vCurField)-len(self.stringQualifier)]
		}
		vRis[vCnt] = Field { value: vCurField , valueType:TYPE_VARCHAR } 
	}
	return vRis,nil
}



func FieldsToStringArray(pFields []Field) []string {
	vRis := make([]string, len(pFields))
	
		
	for vCnt,vCurField := range pFields {
		vRis[vCnt] = vCurField.value 
	}
	
	return vRis
}
