import org.apache.spark.sql.types.IntegerType

// command to start spark: spark-shell --deploy-mode client

// steps: download dataset from https://github.com/serve-and-volley/atp-world-tour-tennis-data/tree/master/csv/3_match_stats
// upload to dataproc
// put it in hdfs
// loading dataset

// dataset is saved in "project_data_match_stats/" in hdfs
val files = "project_data_match_stats/*.csv"
val tennisDF = spark.read.option("header", "false").csv(files)

// clean dataset

// drop missing values
val cleanTennisDF = tennisDF.na.drop()

// datatypes of columns
// cleanTennisDF.printSchema()

// make all the string columns uppercase
val capsDF = cleanTennisDF.columns.foldLeft(cleanTennisDF) { (acc, colName) => acc.withColumn(colName, upper(col(colName)))}

// split the first column in the form "2022-8998-ms001-7-1-mc65-ke29" to only retain the year, i.e. "2022"
val splitDF = capsDF.withColumn("_c0", split($"_c0", "-")(0))

// convert string types to int
val columnsToExclude = Seq("_c1", "_c2", "_c3", "_c5", "_c32")
val columnsToSelect = splitDF.columns.filterNot(columnsToExclude.contains)
val convertedDF = columnsToSelect.foldLeft(splitDF) { (tempDF, colName) => tempDF.withColumn(colName, col(colName).cast(IntegerType))}

// add header to the dataset
val headerList = List("match_id", "tourney_slug", "match_stats_url_suffix", "match_time", "match_duration", "winner_slug", 
"winner_serve_rating", "winner_aces", "winner_double_faults", "winner_first_serves_in", "winner_first_serves_total", 
"winner_first_serve_points_won", "winner_first_serve_points_total", "winner_second_serve_points_won", 
"winner_second_serve_points_total", "winner_break_points_saved", "winner_break_points_serve_total", 
"winner_service_games_played", "winner_return_rating", "winner_first_serve_return_won", "winner_first_serve_return_total",
"winner_second_serve_return_won", "winner_second_serve_return_total", "winner_break_points_converted", 
"winner_break_points_return_total", "winner_return_games_played", "winner_service_points_won", "winner_service_points_total",
"winner_return_points_won", "winner_return_points_total", "winner_total_points_won", "winner_total_points_total", "loser_slug",
"loser_serve_rating", "loser_aces", "loser_double_faults", "loser_first_serves_in", "loser_first_serves_total",
"loser_first_serve_points_won", "loser_first_serve_points_total", "loser_second_serve_points_won", 
"loser_second_serve_points_total", "loser_break_points_saved", "loser_break_points_serve_total", "loser_service_games_played",
"loser_return_rating", "loser_first_serve_return_won", "loser_first_serve_return_total", "loser_second_serve_return_won",
"loser_second_serve_return_total", "loser_break_points_converted", "loser_break_points_return_total", "loser_return_games_played", 
"loser_service_points_won", "loser_service_points_total", "loser_return_points_won", "loser_return_points_total",
"loser_total_points_won", "loser_total_points_total")

val headerDF = convertedDF.toDF(headerList: _*)

// combine all the files into one + save the final dataset in dataproc
headerDF.coalesce(1).write.option("header",true).csv("match_stats_clean/")