import org.apache.spark.sql.types.IntegerType

// command to start spark: spark-shell --deploy-mode client

// load files
val file1 = "matches_merge_first/part-00000-391e4cfc-8025-447b-a2f9-880556c30f5d-c000.csv"
val df1 = spark.read.option("header", "true").csv(file1)
df1.count()
df1.show(5)

val file2 = "atp_matches_clean/part-00000-b394c907-043a-4d9c-8b9b-795666728527-c000.csv"
val df2 = spark.read.option("header", "true").csv(file2)
df2.count()
df2.show(5)

// rename columns so there are no duplicates
val renamedDF2 = df2.toDF(df2.columns.map(_ + "_3"): _*)
renamedDF2.show(5)

// join condition; join when the year, tournament name (tourney_slug) and winner and loser ids match
val joinCondition = df1("match_id") === renamedDF2("tourney_id_3") && df1("tourney_name_2") === renamedDF2("tourney_name_3") && df1("winner_name_2") === renamedDF2("winner_name_3") && df1("loser_name_2") === renamedDF2("loser_name_3")

// Perform the join operation
val mergedDF = df1.join(renamedDF2, joinCondition, "inner")

// check the sizes of the original datasets and the merged ones
mergedDF.count()
df1.count()
renamedDF2.count()

// Show the merged DataFrame
mergedDF.show(1)

// save the final dataset in dataproc
mergedDF.coalesce(1).write.option("header",true).csv("tennis_merged_final/")