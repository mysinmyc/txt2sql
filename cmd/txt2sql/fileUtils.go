package main

import (
	"os"
	"log"
)

func getInputFile( pFileName string ) (*os.File) {

	if  pFileName == "-" {
			log.Println("Reading from standard input");
			return os.Stdin
	}
	
	log.Printf("Opening input file %s...\n",pFileName);
	vFile,vError:=os.Open(pFileName)
	
	if vError != nil  {
		log.Fatal("error opening input file",vError)
	}
	return vFile	
}



func getOutputFile( pFileName string) (*os.File) {

	if pFileName=="-" {
			log.Println("Writing to standard output");
			return os.Stdout
	}
	
	log.Printf("Opening output file %s...\n",pFileName);	
	vFile,vError:= os.Create(pFileName)
	
	if vError != nil  {
		log.Fatal("error opening output file",vError)
	}		
	return vFile
}



func closeFile(pFile *os.File) {

	switch pFile {
		case os.Stdin, os.Stdout:
			return
		default:
			log.Printf("Closing file %s ...\n",pFile.Name());
			pFile.Close()
	}
}