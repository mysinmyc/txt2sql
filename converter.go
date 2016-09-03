package txt2sql



type Converter struct {
	SqlGenerator *SqlGenerator
	parser *Parser
}


func NewConverter(pParser *Parser,pSqlGenerator  *SqlGenerator) *Converter {
	vRis:= &Converter{ SqlGenerator: pSqlGenerator, parser: pParser}
	return vRis
}


func (self *Converter) ConvertRow(pInputLine string ) (string,error) {


	vFields,vParseError:=self.parser.ParseLine(pInputLine)
	
	if vParseError!=nil {
		return self.SqlGenerator.CreateComment("ROW DISCARDED: "+pInputLine),vParseError
	}
	
	vRis,vCreateInsertError:=self.SqlGenerator.CreateInsertFrom(vFields)

	if vCreateInsertError!=nil {
		return self.SqlGenerator.CreateComment("ROW DISCARDED: "+pInputLine),vCreateInsertError
	}
	
	return vRis,nil
}

