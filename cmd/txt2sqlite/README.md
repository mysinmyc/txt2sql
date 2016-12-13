#txt2sqlite

A test to reuse sql generator to load a sqlite local db

# Lesson learned I've learned

* intenally sqlite it's very fast 

* SqLite works fine monothread with atomic operations  ( to be sure SetMaxConnection(1) to serialize access) 


* internally golang sql perform a connection pooling (can be prevented via SetMaxConnection ) that help to reuse the same DB object among multiple goroutines; anyway  it could generate issues for temporary table

* for mass operations it's better to store in a temporary table in memory data and to perform batch commmits of big chunk via insert {destination table} select * from {source table}

* connection sharing it's a mess 


# how to build

`go build github.com/mysinmyc/txt2sql/cmd/txt2sql  && echo ok`	
	

	
# Additional parameters

```
Usage of txt2sqlite:
  -delimiter string
        Field delimiter (default "\t")
  -inFile string
        Input file
  -outDb string
        Output db
  -stringQualifier string
        String qualifier
  -table string
        Destination table
  -threads int
        Threads (default 4)
```


# Extenal dependencies

some components depends on [github.com/mattn/go-sqlite3](https://github.com/mattn/go-sqlite3), and I thanks him for his job. Before using it please check  lincese compatibility for your use cases