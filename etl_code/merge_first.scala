import org.apache.spark.sql.types.IntegerType

// command to start spark: spark-shell --deploy-mode client

// location of shriya's dataset: match_stats_clean/part-00000-ece5c2dd-24e2-4ce2-ada9-1f41d5c3979e-c000.csv
// location of extra dataset 1: match_scores_clean/part-00000-f499e3de-6eec-4b46-90d6-c14f096b82e5-c000.csv

// load both files
val file1 = "match_stats_clean/part-00000-ece5c2dd-24e2-4ce2-ada9-1f41d5c3979e-c000.csv"
val df1 = spark.read.option("header", "true").csv(file1)
df1.count()

val file2 = "match_scores_clean/part-00000-f499e3de-6eec-4b46-90d6-c14f096b82e5-c000.csv"
val df2 = spark.read.option("header", "true").csv(file2)
df2.count()
df2.show(1)

// rename columns so there are no duplicates
val renamedDF2 = df2.toDF(df2.columns.map(_ + "_2"): _*)
renamedDF2.show(1)

// join condition; join when the year, tournament name (tourney_slug) and winner and loser ids match
val joinCondition = df1("match_id") === renamedDF2("tourney_year_id_2") && df1("tourney_slug") === renamedDF2("tourney_slug_2") && df1("winner_slug") === renamedDF2("winner_player_id_2") && df1("loser_slug") === renamedDF2("loser_player_id_2")

// Perform the join operation
val mergedDF = df1.join(renamedDF2, joinCondition, "inner")

// check the sizes of the original datasets and the merged ones
mergedDF.count()
df1.count()
renamedDF2.count()

// retained almost all the columns

// Show the merged DataFrame
mergedDF.show(1)

// save the final dataset in dataproc
mergedDF.coalesce(1).write.option("header",true).csv("matches_merge_first/")