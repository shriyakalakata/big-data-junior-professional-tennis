import org.apache.spark.sql.types.IntegerType

// command to start spark: spark-shell --deploy-mode client

// loading data
val directory = "atp_matches/*.csv"
val df = spark.read.option("header", "true").csv(directory)

// drop columns
val dfAfterDropCols = df.drop("draw_size", "tourney_level", 
	"tourney_date", "match_num", "winner_id", "winner_entry", "winner_ioc",
	"loser_id","loser_entry", "loser_ioc", "winner_seed", "loser_seed", 
	"winner_rank_points", "loser_rank_points")

dfAfterDropCols.show(5)

// remove missing values
val dropNull = dfAfterDropCols.na.drop("any")

//winner_hand and loser_hand also has U = unknown hand which is means its missing so this should be filtered out as well
val dfDrop2 = dropNull.filter(not($"winner_hand".contains("U")) && not($"loser_hand".contains("U")))

// modified: split first column
val dfModified = dfDrop2.withColumn("tourney_id", split($"tourney_id", "-")(0))

val numeric_columns = List("tourney_id", "winner_ht", "loser_ht", "winner_age", "loser_age", "best_of", "minutes", 
	"w_ace", "w_df", "w_svpt", "w_1stIn", "w_1stWon", "w_2ndWon", 
    "w_SvGms", "w_bpSaved", "w_bpFaced", "l_ace", "l_df", "l_svpt", "l_1stIn", "l_1stWon",
    "l_2ndWon", "l_SvGms", "l_bpSaved", "l_bpFaced", "winner_rank",
    "loser_rank")


// converting numeric columns to integer
val convertedToIntDataFrame = numeric_columns.foldLeft(dfModified) { (df, colName) =>
    df.withColumn(colName + "_int", col(colName).cast("int")).drop(colName).withColumnRenamed(colName + "_int", colName)
}

//l_bpsaved negative values filtered out
val nonNegativeFilter = convertedToIntDataFrame.filter($"l_bpSaved" >= 0)

nonNegativeFilter.show()

val colToBeTextFormatted = List("tourney_name", "surface", "winner_name", "loser_name")

val dfAfterLowerCase = colToBeTextFormatted.foldLeft(nonNegativeFilter) { (df, colName) =>
    df.withColumn(colName + "_lower", upper(col(colName))).drop(colName).withColumnRenamed(colName + "_lower", colName)
}

//2. Create a binary column based on the condition of another column. 

//  Condition if winner has more aces than loser (w_ace > l_ace), then new column value is 1 otherwise 0

val dfAfterBinaryCol1 = dfAfterLowerCase.withColumn("winner_more_aces", when($"w_ace" > $"l_ace", 1).otherwise(0))

val dfAfterBinaryCol2 = dfAfterBinaryCol1.withColumn("loser_more_doublefault", when($"l_df" > $"w_df", 1).otherwise(0))

dfAfterBinaryCol2.coalesce(1).write.option("header",true).csv("atp_matches_clean/")