# txt2sql

generate SQL statements to insert text file into databases

Sometimes happens that i need to insert some text data into the database fast and without any ETL tools.

The solution is:

`cat {myFile} | txt2sql --table {tableName} > output.sql`
	
This program export generate sql to put data coming from a text file into a table

At the moment it consider all the fields as strings

First row must contains columns names


# how to build

`go build github.com/mysinmyc/txt2sql/cmd/txt2sql  && echo ok`	
	

	
# Additional parameters

```
Usage of ./txt2sql:
  -db string
        Db Type {oracle|sqlserver|mysql} (default "mysql")
  -delimiter string
        Field delimiter (default "\t")
  -in string
        Input file - for standard input (default "-")
  -noddl
        Don't Generate table ddl
  -out string
        Output file or - for standard output (default "-")
  -stringQualifier string
        String qualifier
  -table string
        Destination table (default "__TABLE__")
```
  



# Status of the project

At the moment this no more than an exercise and an execuse to write in golang language. No tests have been performed

