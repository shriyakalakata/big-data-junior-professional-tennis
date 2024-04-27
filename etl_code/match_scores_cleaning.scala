import org.apache.spark.sql.types.IntegerType

// command to start spark: spark-shell --deploy-mode client

// ----------------------- EXTRA DATASET 1 CLEANING -----------------------

// steps: download dataset from https://github.com/serve-and-volley/atp-world-tour-tennis-data/tree/master/csv/2_match_scores
// upload to dataproc
// put it in hdfs

import org.apache.spark.sql.types.IntegerType

// loading dataset
val files = "match_scores/*.csv"
val df = spark.read.option("header", "false").csv(files)

// add header to the dataset
val headerList = List("tourney_year_id", "tourney_order", "tourney_name", "tourney_slug", "tourney_url_suffix", "start_date",
"start_year", "start_month", "start_day", "end_date", "end_year", "end_month", "end_day", "currency", "prize_money", 
"match_index", "tourney_round_name", "round_order", "match_order", "winner_name", "winner_player_id", "winner_slug", 
"loser_name", "loser_player_id", "loser_slug", "winner_seed", "loser_seed", "match_score_tiebreaks", "winner_sets_won",
"loser_sets_won", "winner_games_won", "loser_games_won", "winner_tiebreaks_won", "loser_tiebreaks_won", "match_id", 
"match_stats_url_suffix")

val headerDF = df.toDF(headerList: _*)

// drop unneccessary columns
// don't need start and end dates for tournaments. only looking at year, which is preserved in a different column
// not using prize money information
// dropping seeds as only a part of players have a seed in the tournament
val columnsToDrop = Seq("start_date", "start_year", "start_month", "start_day", "end_date", "end_year", "end_month", "end_day", 
"currency", "prize_money", "winner_seed", "loser_seed", "match_id", "match_stats_url_suffix")
val droppedDF = headerDF.drop(columnsToDrop: _*)

// check how many missing values are in each column
val missingValueCounts = droppedDF.columns.map(colName => (colName, droppedDF.filter(droppedDF(colName).isNull || droppedDF(colName) === "").count()))
missingValueCounts.foreach(println)

// drop missing values
val df2 = droppedDF.na.drop()

// check for duplicates

val uniqueMatchIdCount = df2.distinct().count()
val totalRowCount = df2.count()
val hasDuplicates = totalRowCount != uniqueMatchIdCount

// no duplicates

// make all the string columns uppercase
val capsDF = df2.columns.foldLeft(df2) { (acc, colName) => acc.withColumn(colName, upper(col(colName)))}

// split the first column
val splitDF = capsDF.withColumn("tourney_year_id", split($"tourney_year_id", "-")(0))

// convert string types to int
val columnsToSelect = Seq("tourney_year_id", "tourney_order", "round_order", "match_order", "winner_sets_won", "loser_sets_won", 
"winner_games_won", "loser_games_won", "winner_tiebreaks_won", "loser_tiebreaks_won")
val convertedDF = columnsToSelect.foldLeft(splitDF) { (tempDF, colName) => tempDF.withColumn(colName, col(colName).cast(IntegerType))}

// combine all the files into one + save the final dataset in dataproc
convertedDF.coalesce(1).write.option("header",true).csv("match_scores_clean/")