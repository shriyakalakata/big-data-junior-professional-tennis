val fileName = "project_data_match_stats/match_stats_2022.csv"
val tennisRRD = sc.textFile(fileName)

// Count the number of records. 
val numRecords = tennisRRD.count()

// Map the records to a key and a value and count the number of records using map() function

val uniqueRecordCount = tennisRRD.map(record => (record, 1)).distinct().count()  

// Find the distinct values in each column (columns you are using for your analytic).

val tennisDF = spark.read.option("header", "false").csv(fileName)

(0 until tennisDF.columns.length).foreach { colIndex =>
  val colName = s"Column_$colIndex"
  println(s"Distinct values for column $colName:")
  tennisDF.selectExpr(s"_c$colIndex").distinct().limit(10).show(false)
}