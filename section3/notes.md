Dataframe extends RDD (are now used instad of)
Datframes:
  - contain row objects
  - can run sql queries
    but can also run other commands like show(), select(), filter(), groupBy()
    and convert to an rdd rdd() to use map etc..
    - MLLib and Spark  Straming are moving toward using DataFrames
  - can have a schema (leading to more efficient storage)
  - easy to read and write from/to different file formats: Json, Hive, parquet, csv, ..
  - supports JDBC/ODBC tablbleau

DataSet (more Scala than python):
 - Dataframes are actually Dataset of Row Objects (untyped)
 - Dataset can wrap known, typed data too (but not much relevant in python since it's mostly untyped)
   so you want to use them if you are using a typed language

Shell Access:
 - Spark SQL exposes a JDBC/ODBC server (if you build Spark with Hive support)
   so you can have an sql command line
   Note the advantage is that it's not a local DB but it has a cluster of machines behind the scenes
 - start it with sbin/start-thriftserver.sh
 - Listens on port 10000 by default
 - connect using bin/beeline -u jdbc:hive2://localhost:10000
 - you can also define functions (UDFs) and register as "user defined"
   then you can refer to them in your sql commands
