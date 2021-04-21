try:
  spark.sql("ALTER TABLE schema.TABLENAME ADD columns (COLUMNAME string) ")
except:
  print("ERROR in: ALTER TABLE ")