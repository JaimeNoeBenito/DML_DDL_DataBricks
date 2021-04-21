table = "yourTablename"

#Step 1: read the table
spark.sql("REFRESH TABLE your_schema." + table)
df = spark.sql("select * from your_schema." +table) 


#Step 2:  Do your thins here (delete ... additions)

df = df.where(F.col("PK_Column").isNotNull())

from pyspark.sql import functions as F
df = df.withColumn("New_colum", F.lit("String value ")) # new string colum


#Step 3: create temporal file 
df.repartition("repartition_field").write.partitionBy("repartition_field").option("path", "/your_path/"+ table + "temp").saveAsTable("your_schema." + table + "temp",format='parquet', mode='overwrite',option='snappy')

# Step 4: remove current storage
if dirExists("/your_path/"+ table + '/'):
  dbutils.fs.rm("/your_path/"+ table + '/', True)
  
# Step 5: drop (hive) table from your schema
spark.sql("DROP TABLE your_schema." + table)

# Step 6: read new table
df = spark.sql("select * from your_schema." + table + "temp")
df.repartition("repartition_field").write.partitionBy("repartition_field").option("path","/your_path/"+ table).saveAsTable("your_schema." + table, format='parquet', mode='overwrite',option='snappy')

#Step 7: remove temporal file
dbutils.fs.rm("/your_path/"  + table + 'temp/', True)

#Step 8: Drop temporal table
spark.sql("DROP TABLE staging." + table + "temp")

#Step 9: refresh new table
spark.sql("REFRESH TABLE staging." + table) 