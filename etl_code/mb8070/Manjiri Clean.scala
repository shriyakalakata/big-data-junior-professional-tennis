//------------------LOADING DATA-------------------------
val directory = "hw6files/*.csv"

val df = spark.read.option("header", "true").csv(directory)

println("Number of Records Before Cleaning: "+df.count())
println("\n")

//------------------DATA CLEANING------------------------

//------Drop columns you do not need, add columns with uniform values (if needed), 

//Columns not needed: "tourney_id", "tourney_name", "draw_size", "tourney_level", 
//	"tourney_date", "match_num", "winner_id", "winner_entry", "winner_ioc",
//	"loser_id","loser_entry", "loser_ioc"

//winner_ioc, loser_ioc: tennis is an individual dependent sport and not related to country so dropping that column

//winner_entry and loser_entry are not informative of player style, it is just a label used for them, winner_rank changes throughout the tournament, while seed is their overall rank so dropping those columns as well

//Removed loser_seed and winner_seed columns as they are tournament specific and vary; winner_rank is a better measurement of a players ranking (A lot of players are unseeded (only top 32 players are seeded), therefore many null value thus dropping seed columns

val dfAfterDropCols = df.drop("tourney_id", "tourney_name", "draw_size", "tourney_level", 
	"tourney_date", "match_num", "winner_id", "winner_entry", "winner_ioc",
	"loser_id","loser_entry", "loser_ioc", "winner_seed", "loser_seed", 
	"winner_rank_points", "loser_rank_points") //removed as they are linear to winner_rank and loser_rank columns

dfAfterDropCols.show(5)

//------NULL VALUES / MISSING VALUES: drop rows if they are NULL if you decide they aren't to be included.

//Columns with null: surface, winner_seed, winner_entry, winner_hand, winner_ht, loser_seed, loser_hand, loser_ht, w_ace, w_df, w_svpt, w_2ndWon, w_SvGms, w_bpSaved, w_bpFaced, l_ace, l_df, l_2ndWon, l_SvGms, l_bpSaved, l_bpFaced, "winner_rank_points", "loser_rank_points"

//Want to drop any row where remaining column contains null values
val dropNull = dfAfterDropCols.na.drop("any")

println("Number of Records After Dropping Nulls: "+dropNull.count())

//winner_hand and loser_hand also has U = unknown hand which is means its missing so this should be filtered out as well
val dfDrop2 = dropNull.filter(not($"winner_hand".contains("U")) && not($"loser_hand".contains("U")))

//CHECKING if cleaning worked so far
println("Number of Records After Dropping U's: "+ dfDrop2.count())
dfDrop2.select("winner_hand").distinct.show(false)
dfDrop2.select("loser_hand").distinct.show(false)

//From CountRecs.scala inspection, l_bpSaved has negative values (it should be positive thus represents error)
//Other numeric columns may have this same issue or similar issue (height = 0) too, so only keeping integer columns where value >= 0, when appropriate

//INSPECTION
//Finding min max of numeric columns (to see which columns have negative values)

println("Finding min and max for numeric columns\n")

val numeric_columns = List("winner_ht", "loser_ht", "winner_age", "loser_age", "best_of", "minutes", 
	"w_ace", "w_df", "w_svpt", "w_1stIn", "w_1stWon", "w_2ndWon", 
    "w_SvGms", "w_bpSaved", "w_bpFaced", "l_ace", "l_df", "l_svpt", "l_1stIn", "l_1stWon",
    "l_2ndWon", "l_SvGms", "l_bpSaved", "l_bpFaced", "winner_rank",
    "loser_rank")

for (col <- numeric_columns) {
  println(s"Min and Max values for column '$col':")
  dfDrop2.agg(min(col), max(col)).show()
}

//ISSUES FOUND:
//Found that minutes max value is 99 when the dataframe has values over 99. Similar issues are possible for other columns as well
//Also found that winner_age and loser_age columns are floats. but logically it should be integer, so we have to transform this column and remove the decimal points
//Found that l_bpsaved has negative values so filtering such that. No other logical errors were found (e.g. 0 for height)

// Function to convert numeric columns to integers (will solve first two issues)
val convertedToIntDataFrame = numeric_columns.foldLeft(dfDrop2) { (df, colName) =>
    df.withColumn(colName + "_int", col(colName).cast("int")).drop(colName).withColumnRenamed(colName + "_int", colName)
}

//Checking 

println("Finding min and max for columns after casting numeric columns to integers\n")

for (col <- numeric_columns) {
  println(s"Min and Max values for column '$col':")
  convertedToIntDataFrame.agg(min(col), max(col)).show() //Notice how now the min max values for several columns changed
}

//Checking age columns now
println("Checking if age columns are now integers\n")
convertedToIntDataFrame.select("winner_age", "loser_age").show()

//Even after converting to int now only l_bpsaved column has negative values

//l_bpsaved negative values filtered out
val nonNegativeFilter = convertedToIntDataFrame.filter($"l_bpSaved" >= 0)

//Checking
println("Checking if negative breakpoints values have been dropped\n")
nonNegativeFilter.agg(min("l_bpSaved"), max("l_bpSaved")).show() //Now min is 0 instead of -6
print("Number of Records after dropping rows with negative breakpoints: "+nonNegativeFilter.count())
println("\n")

//------OTHER PROBLEMS:

//1. Logical Fallacies

println("Checking how many logical fallacies there are\n")

//Checking if l_bpsaved > l_bp_faced (incorrect logic) how many break points one saves cannot be more than faced
println("Number of rows where l_bpSaved > l_bpFaced: " + nonNegativeFilter.filter($"l_bpSaved" > $"l_bpFaced").count())

//Checking if w_bpsaved > w_bp_faced (incorrect logic)
println("Number of rows where w_bpSaved > w_bpFaced: " + nonNegativeFilter.filter($"w_bpSaved" > $"w_bpFaced").count())

//There were logical fallacies before casting the numeric columns to ints but not anymore.

//DUPLICATES: From previous analysis (CountRecs) all records are unique (no duplicates)

//Seeing if there are string columns with unsual lengths (e.g. name of 1 length/empty string)
val shortestString1 = nonNegativeFilter.select("winner_name").orderBy(length(col("winner_name"))).first().getString(0)

val longestString1= nonNegativeFilter.select("winner_name").orderBy(length(col("winner_name")).desc).first().getString(0)

println("Shortest winner name: "+ shortestString1)
println("Longest winner name: "+ longestString1)

val shortestString2 = nonNegativeFilter.select("loser_name").orderBy(length(col("loser_name"))).first().getString(0)

val longestString2 = nonNegativeFilter.select("loser_name").orderBy(length(col("loser_name")).desc).first().getString(0)

println("Shortest loser name: "+ shortestString2)
println("Longest loser name: "+ longestString2)

//Nothing looks off so now we can proceed to final inspection

//-------------AFTER CLEANING FINAL INSPECTION-----------------
println("\nNumber of Records After All Cleaning: "+ nonNegativeFilter.count()) //interAgesdF is the final dF for now

nonNegativeFilter.show() //showing final df for now

//Writing df to hdfs 
nonNegativeFilter.write.option("header",true).csv("cleanDataHw6/")


