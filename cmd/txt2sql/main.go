package main


import (
	"flag"
	"os"
	"bufio"
	"github.com/mysinmyc/txt2sql"
	"log"
)




func main() {

	vParameterIn:=flag.String("in", "-", "Input file - for standard input")
	vParameterOut:=flag.String("out", "-", "Output file or - for standard output")
	vParameterDelimiter:=flag.String("delimiter", "\t", "Field delimiter")
	vParameterTableName:=flag.String("table", "__TABLE__", "Destination table")
	vParameterStringQualifier:=flag.String("stringQualifier", "", "String qualifier")
	vParameterDbType:=flag.String("db","mysql","Db Type {oracle|sqlserver|mysql}")
	vParameterNoDdl:=flag.Bool("noddl",false,"Don't Generate table ddl")
	flag.Parse()	

	vFileIn:= getInputFile(*vParameterIn)
	defer closeFile(vFileIn)

	vFileOut:= getOutputFile(*vParameterOut)
	defer closeFile(vFileOut)

	vScanner := bufio.NewScanner(vFileIn)
	
	vParser := txt2sql.NewParser(*vParameterDelimiter,*vParameterStringQualifier)

	
	if !vScanner.Scan() {
		log.Fatal("Input file empty");
		os.Exit(10)
	}
	vColumnsParsed,_:=vParser.ParseLine(vScanner.Text())
	vColumnsName:=txt2sql.FieldsToStringArray(vColumnsParsed)
		
	vSqlGenerator := txt2sql.NewSqlGenerator(*vParameterDbType, *vParameterTableName, vColumnsName)
	vConverter := txt2sql.NewConverter(vParser,vSqlGenerator)

	
	if *vParameterNoDdl{
		log.Printf("No ddl included")
	}else {
		vFileOut.WriteString(vSqlGenerator.CreateTableDdl())
		vFileOut.WriteString("\n")
	}
	
	vConverted:=0
	vDiscarded:=0
	for vScanner.Scan() { 
		vCurRow,vError:=vConverter.ConvertRow(vScanner.Text())
		
		if vError ==nil {
			vConverted++;
		} else {
			vDiscarded++;
		}  
		vFileOut.WriteString(vCurRow)
		vFileOut.WriteString("\n")
	}
	
	log.Printf("Completed. Rows converted: %d discarded %d\n",vConverted,vDiscarded) 

	if vDiscarded > 0 {
		os.Exit(10)
	}
	
}
