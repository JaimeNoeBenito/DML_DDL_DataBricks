#Step 1 - Drop table
try:
  spark.sql("drop table schema.tableName")
except:
  print("Table does not exist")
  
try:
  spark.sql("drop table schema.tableName")
except:
  print("Table does not exist")


#Step 2 - create new tabe (here you can delete or add new fields
spark.sql ("""CREATE TABLE SCHEMA.TABLENAME (     
  PK_FIELD              string not null,
  STRING_FIELD          string,
  NUMBER_FIELD          double,
  DATE_FIELD          	date,
  PARITION_FIELD          whateve_type
 )                                  
USING DELTA                      
PARTITIONED BY (PARITION_FIELD)                  
LOCATION '"""+ENV+"""/your/Path'
""")

#Step 3 run repair table if necessary  
spark.sql("MSCK REPAIR TABLE SCHEMA.TABLENAME")


# Refresh table
spark.sql("refresh table SCHEMA.TABLENAME")

