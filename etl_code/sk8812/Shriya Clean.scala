val fileName = "project_data_match_stats/match_stats_2022.csv"
val tennisDF = spark.read.option("header", "false").csv(fileName)

// Drop columns you do not need, - drop the 3rd and 4th columns. save it as a new dataset
val filteredTennisDF = tennisDF.drop("_c2", "_c3")

// Drop rows if they are NULL if you decide they aren't to be included. if any value is missing or empty or null, drop that entire column
val cleanTennisDF = filteredTennisDF.na.drop()

// for the first column, split by "-" and replace the entire string with just the first part of the split
val modifiedTennisDF = cleanTennisDF.withColumn("_c0", split(col("_c0"), "-").getItem(0))
