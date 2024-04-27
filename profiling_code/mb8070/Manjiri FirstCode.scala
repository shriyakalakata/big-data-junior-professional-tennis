//----------------------------CODE ANALYSIS--------------------------------

val directory = "cleanDataHw6/*.csv"

val df = spark.read.option("header", "true").csv(directory)

//Mean, median, and mode of numerical data 

println("Finding mean, median, and mode, and stddev for numeric columns\n")

val numeric_columns = List("winner_ht", "loser_ht", "winner_age", "loser_age", "minutes", 
	"w_ace", "w_df", "w_svpt", "w_1stIn", "w_1stWon", "w_2ndWon", 
    "w_SvGms", "w_bpSaved", "w_bpFaced", "l_ace", "l_df", "l_svpt", "l_1stIn", "l_1stWon",
    "l_2ndWon", "l_SvGms", "l_bpSaved", "l_bpFaced", "winner_rank",
    "loser_rank")


val convertedToIntDataFrame = numeric_columns.foldLeft(df) { (df, colName) =>
    df.withColumn(colName + "_int", col(colName).cast("int")).drop(colName).withColumnRenamed(colName + "_int", colName)
}

//Mean and median won't be calculated for best_of column (as it is numeric categorical)

for (col <- numeric_columns) {
  println(s"Statistics for column '$col':")

  //Calculating the mean, median, mode, stdev
  val mean = convertedToIntDataFrame.select(avg(col)).first().getDouble(0)
  val median = convertedToIntDataFrame.stat.approxQuantile(col, Array(0.5), 0.0)(0)
  val mode = convertedToIntDataFrame.groupBy(col).count().orderBy(desc("count")).first().get(0)
  val stDev = convertedToIntDataFrame.agg(stddev(convertedToIntDataFrame(col))).first().getDouble(0)

  println(s"Mean: $mean")
  println(s"Median: $median")
  println(s"Mode: $mode")
  println(s"Standard Deviaion: $stDev \n")

}

//Only mode will be calculated for best_of column

println(s"Statistics for column 'best_of':")
val mode_bo = convertedToIntDataFrame.groupBy("best_of").count().orderBy(desc("count")).first().get(0)
println(s"Mode: $mode_bo")

//----------------------------CODE CLEANING--------------------------------


//1. Text formatting: making the winner and loser name columns all lowercase for normalization 

print("Before text formatting: \n")

convertedToIntDataFrame.select("surface", "winner_name", "loser_name").show(5)

val colToBeTextFormatted = List("surface", "winner_name", "loser_name")

val dfAfterLowerCase = colToBeTextFormatted.foldLeft(convertedToIntDataFrame) { (df, colName) =>
    df.withColumn(colName + "_lower", upper(col(colName))).drop(colName).withColumnRenamed(colName + "_lower", colName)
}

print("After text formatting: \n")
dfAfterLowerCase.select("surface", "winner_name", "loser_name").show(5)


//2. Create a binary column based on the condition of another column. 

//  Condition if winner has more aces than loser (w_ace > l_ace), then new column value is 1 otherwise 0

val dfAfterBinaryCol1 = dfAfterLowerCase.withColumn("winner_more_aces", when($"w_ace" > $"l_ace", 1).otherwise(0))

//Checking
print("winner_more_aces column inspection: \n")
dfAfterBinaryCol1.select("w_ace", "l_ace", "winner_more_aces").show(5)

//  Condition if loser has more double faults than winner (l_df > w_df), then new column value is 1 otherwise 0

val dfAfterBinaryCol2 = dfAfterBinaryCol1.withColumn("loser_more_doublefault", when($"l_df" > $"w_df", 1).otherwise(0))

print("loser_more_doublefault column inspection: \n")

//Checking
dfAfterBinaryCol2.select("w_df", "l_df", "loser_more_doublefault").show(5)

//Writing the cleaned data to new directory
dfAfterBinaryCol2.coalesce(1).write.option("header",true).csv("cleanDataHw7/")

