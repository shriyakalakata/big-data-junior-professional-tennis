import org.apache.spark.sql.types.IntegerType

// command to start spark: spark-shell --deploy-mode client

// load the dataset
val file = "tennis_merged_final/part-00000-baea738d-95aa-4f4b-b952-02b19f4e3fbe-c000.csv"
val df = spark.read.option("header", "true").csv(file)

// look at columns and one row to see what's redundant
df.columns
df.show(1)

// current columns:
// |match_id|tourney_slug|match_stats_url_suffix|match_time|match_duration|winner_slug|winner_serve_rating|winner_aces|
// winner_double_faults|winner_first_serves_in|winner_first_serves_total|winner_first_serve_points_won|
// winner_first_serve_points_total|winner_second_serve_points_won|winner_second_serve_points_total|winner_break_points_saved|
// winner_break_points_serve_total|winner_service_games_played|winner_return_rating|winner_first_serve_return_won|
// winner_first_serve_return_total|winner_second_serve_return_won|winner_second_serve_return_total|
// winner_break_points_converted|winner_break_points_return_total|winner_return_games_played|winner_service_points_won|
// winner_service_points_total|winner_return_points_won|winner_return_points_total|winner_total_points_won|
// winner_total_points_total|loser_slug|loser_serve_rating|loser_aces|loser_double_faults|loser_first_serves_in|
// loser_first_serves_total|loser_first_serve_points_won|loser_first_serve_points_total|loser_second_serve_points_won|
// loser_second_serve_points_total|loser_break_points_saved|loser_break_points_serve_total|loser_service_games_played|
// loser_return_rating|loser_first_serve_return_won|loser_first_serve_return_total|loser_second_serve_return_won|
// loser_second_serve_return_total|loser_break_points_converted|loser_break_points_return_total|loser_return_games_played|
// loser_service_points_won|loser_service_points_total|loser_return_points_won|loser_return_points_total|loser_total_points_won|
// loser_total_points_total|tourney_year_id_2|tourney_order_2|tourney_name_2|tourney_slug_2|tourney_url_suffix_2|match_index_2|
// tourney_round_name_2|round_order_2|match_order_2|winner_name_2|winner_player_id_2|winner_slug_2|loser_name_2|
// loser_player_id_2|       loser_slug_2|match_score_tiebreaks_2|winner_sets_won_2|loser_sets_won_2|winner_games_won_2|
// loser_games_won_2|winner_tiebreaks_won_2|loser_tiebreaks_won_2|winner_hand_3|loser_hand_3|score_3|round_3|tourney_id_3|
// winner_ht_3|loser_ht_3|winner_age_3|loser_age_3|best_of_3|minutes_3|w_ace_3|w_df_3|w_svpt_3|w_1stIn_3|w_1stWon_3|w_2ndWon_3|
// w_SvGms_3|w_bpSaved_3|w_bpFaced_3|l_ace_3|l_df_3|l_svpt_3|l_1stIn_3|l_1stWon_3|l_2ndWon_3|l_SvGms_3|l_bpSaved_3|l_bpFaced_3|
// winner_rank_3|loser_rank_3|tourney_name_3|surface_3|winner_name_3|       loser_name_3|winner_more_aces_3|
// loser_more_doublefault_3|

// first round of removing redundant columns
val columnsToDrop = Seq("match_stats_url_suffix", "match_time", "match_duration", "winner_slug", "loser_slug", 
"tourney_year_id_2", "tourney_order_2", "tourney_name_2", "tourney_slug_2", "tourney_url_suffix_2", "match_index_2", 
"tourney_round_name_2", "round_order_2", "match_order_2", "winner_player_id_2", "winner_slug_2", "loser_player_id_2", 
"loser_slug_2", "tourney_id_3", "tourney_name_3", "winner_name_3", "loser_name_3")

val df1 = df.drop(columnsToDrop: _*)

// second round of removing redundant columns

val columnsToDropMore = Seq("winner_serve_rating", "winner_first_serves_in", "winner_first_serves_total", "winner_return_rating", "winner_total_points_total", "loser_serve_rating", "loser_first_serves_in", "loser_first_serves_total", "loser_return_rating", "loser_total_points_total", "match_score_tiebreaks_2", "winner_sets_won_2", "loser_sets_won_2", "winner_hand_3", "loser_hand_3", "score_3", "round_3", "best_of_3", "minutes_3", "w_ace_3", "w_df_3", "w_svpt_3", "w_1stIn_3", "w_1stWon_3", "w_2ndWon_3", "w_SvGms_3", "w_bpSaved_3", "w_bpFaced_3", "l_ace_3", "l_df_3", "l_svpt_3", "l_1stIn_3", "l_1stWon_3", "l_2ndWon_3", "l_SvGms_3", "l_bpSaved_3", "l_bpFaced_3")

val df2 = df1.drop(columnsToDropMore: _*)

// create new columns using the existing columns
// round the new percentage values to two decimal places

// new column 1: winner_first_serve_return_won /  winner_first_serve_return_total —> winner_first_serve_return_won_percentage
val dfcol1 = df2.withColumn("winner_first_serve_return_won_percentage", round(col("winner_first_serve_return_won") / col("winner_first_serve_return_total") * 100, 2))

// check if the column was added correctly
dfcol1.show(1)

// check if the new column was correctly calculated
dfcol1.select("winner_first_serve_return_won", "winner_first_serve_return_total", "winner_first_serve_return_won_percentage").show(10) 

// new column 2: winner_second_serve_return_won /  winner_second_serve_return_total —> winner_second_serve_return_won_percentage
val dfcol2 = dfcol1.withColumn("winner_second_serve_return_won_percentage", round(col("winner_second_serve_return_won") / col("winner_second_serve_return_total") * 100, 2))

// new column 3: winner_service_points_won /  winner_service_points_total —> winner_service_points_won_percentage
val dfcol3 = dfcol2.withColumn("winner_service_points_won_percentage", round(col("winner_service_points_won") / col("winner_service_points_total") * 100, 2))

// new column 4: winner_return_points_won /  winner_return_points_total —> winner_return_points_won_percentage
val dfcol4 = dfcol3.withColumn("winner_return_points_won_percentage", round(col("winner_return_points_won") / col("winner_return_points_total") * 100, 2))

// new column 6: loser_first_serve_return_won /  loser_first_serve_return_total —> loser_first_serve_return_won_percentage
val dfcol6 = dfcol4.withColumn("loser_first_serve_return_won_percentage", round(col("loser_first_serve_return_won") / col("loser_first_serve_return_total") * 100, 2))

// new column 7: loser_second_serve_return_won /  loser_second_serve_return_total —> loser_second_serve_return_won_percentage
val dfcol7 = dfcol6.withColumn("loser_second_serve_return_won_percentage", round(col("loser_second_serve_return_won") / col("loser_second_serve_return_total") * 100, 2))

// new column 8: loser_service_points_won /  loser_service_points_total —> loser_service_points_won_percentage
val dfcol8 = dfcol7.withColumn("loser_service_points_won_percentage", round(col("loser_service_points_won") / col("loser_service_points_total") * 100, 2))

// new column 9: loser_return_points_won / loser_return_points_total —> loser_return_points_won_percentage
val dfcol9 = dfcol8.withColumn("loser_return_points_won_percentage", round(col("loser_return_points_won") / col("loser_return_points_total") * 100, 2))

dfcol9.show(1)

// ensure none of the new columns have values < 0 and > 100

// Define the list of columns
val columnsToCheck = Seq("winner_first_serve_return_won_percentage", "winner_second_serve_return_won_percentage", "winner_service_points_won_percentage", "winner_return_points_won_percentage", "loser_first_serve_return_won_percentage", "loser_second_serve_return_won_percentage", "loser_service_points_won_percentage", "loser_return_points_won_percentage")

// function to check values greater than 100 or less than 0
def countOutOfRangeValues(columnName: String): Long = {dfcol9.filter(col(columnName) > 100 || col(columnName) < 0).count()}

// Iterate over the columns and print the count of out-of-range values
columnsToCheck.foreach { columnName => 
val count = countOutOfRangeValues(columnName) 
println(s"Column '$columnName': $count values greater than 100 or less than 0")
}

// we see that the following columns have values out of bound: "loser_second_serve_return_won_percentage"

val col1 = "loser_second_serve_return_won_percentage"

// look at the rows with out of bound values
val rowsOutOfRange = dfcol9.filter(col(col1) > 100 || col(col1) < 0)

rowsOutOfRange.show()

// drop the new columns which have values > 100 or < 0

dfcol9.count()

val filteredDF = dfcol9.filter(!(col(col1) < 0 || col(col1) > 100))

filteredDF.count()

// check again to see if the new columns are in bound

def countOutOfRangeValues2(columnName: String): Long = {filteredDF.filter(col(columnName) > 100 || col(columnName) < 0).count()}

columnsToCheck.foreach { columnName => 
val count = countOutOfRangeValues2(columnName) 
println(s"Column '$columnName': $count values greater than 100 or less than 0")
}


// check how many missing values are in each column
val missingValueCounts = filteredDF.columns.map(colName => (colName, filteredDF.filter(filteredDF(colName).isNull || filteredDF(colName) === "").count()))
missingValueCounts.foreach(println)

val filteredDF2 = filteredDF.na.drop()

// check for duplicate values
val uniqueMatchIdCount = filteredDF2.distinct().count()
val totalRowCount = filteredDF2.count()
val hasDuplicates = totalRowCount != uniqueMatchIdCount

// remove duplicates
val dfWithoutDuplicates = filteredDF2.dropDuplicates()

dfWithoutDuplicates.show(2)

// save the final dataset in dataproc
dfWithoutDuplicates.coalesce(1).write.option("header",true).csv("tennis_merged_final_clean/")
