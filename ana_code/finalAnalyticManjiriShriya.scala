import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val directory = "tennis_merged_final_clean/*.csv" //This will be changed to the merged dataset (just getting code done for now)

val df = spark.read.option("header", "true").csv(directory)

//Converting columns to numeric_columns int

val numeric_columns = List("winner_aces", "winner_double_faults", "winner_first_serve_points_won", 
	"winner_first_serve_points_total", "winner_second_serve_points_won", "winner_second_serve_points_total", 
	"winner_break_points_saved", "winner_break_points_serve_total", "winner_service_games_played", 
	"winner_first_serve_return_won", "winner_first_serve_return_total", "winner_second_serve_return_won", 
	"winner_second_serve_return_total", "winner_break_points_converted", "winner_break_points_return_total", 
	"winner_return_games_played", "winner_service_points_won", "winner_service_points_total", 
	"winner_return_points_won", "winner_return_points_total", "winner_total_points_won", 
	"loser_aces", "loser_double_faults", "loser_first_serve_points_won", "loser_first_serve_points_total", 
	"loser_second_serve_points_won", "loser_second_serve_points_total", "loser_break_points_saved", 
	"loser_break_points_serve_total", "loser_service_games_played", "loser_first_serve_return_won", 
	"loser_first_serve_return_total", "loser_second_serve_return_won", "loser_second_serve_return_total", 
	"loser_break_points_converted", "loser_break_points_return_total", "loser_return_games_played", 
	"loser_service_points_won", "loser_service_points_total", "loser_return_points_won", "loser_return_points_total", 
	"loser_total_points_won", "winner_games_won_2", "loser_games_won_2", "winner_tiebreaks_won_2", 
	"loser_tiebreaks_won_2", "winner_ht_3", "loser_ht_3", "winner_age_3", "loser_age_3", 
	"winner_rank_3", "loser_rank_3")


val cleanDf = numeric_columns.foldLeft(df) { (df, colName) =>
    df.withColumn(colName + "_int", col(colName).cast("int")).drop(colName).withColumnRenamed(colName + "_int", colName)
}


//#################################COMPARISON BASED ON WIN PERCENTAGE#########################################

//***************************WITHIN JUNIORS*********************************

//Creating a new column of win percentage
val winnerIsJuniorDf = cleanDf.filter($"winner_age_3"<=18)

val loserIsJuniorDf = cleanDf.filter($"loser_age_3"<=18)

val numOfWinsJunior = winnerIsJuniorDf.groupBy("winner_name_2").agg(count("*").alias("wins")).orderBy(desc("wins"))

val numOfLossesJunior = loserIsJuniorDf.groupBy("loser_name_2").agg(count("*").alias("losses")).orderBy(desc("losses"))

val joinedJuniorStats = numOfWinsJunior.join(numOfLossesJunior, numOfWinsJunior("winner_name_2") === numOfLossesJunior("loser_name_2"), "inner").select(numOfWinsJunior("winner_name_2").alias("player_name"), numOfWinsJunior("wins").alias("wins"), numOfLossesJunior("losses").alias("losses"))

val winLossPrecentageJunior = joinedJuniorStats.withColumn("win_percentage", col("wins") / (col("wins")+col("losses")))


//Players with highest win percentage
val topJunior = winLossPrecentageJunior.orderBy(desc("win_percentage"))
println("Juniors with Highest Win Percentage")
topJunior.show()

//Players with lowest win percentage
val bottomJunior = winLossPrecentageJunior.orderBy(asc("win_percentage"))
println("Juniors with Lowest Win Percentage")
bottomJunior.show()


println("\n Analyzing Top Junior vs Worst Junior")

//Highest wins percentage junior

val mostWinsJunior = winLossPrecentageJunior.orderBy(desc("win_percentage")).head().getString(0)

val lowestWinsJunior = winLossPrecentageJunior.orderBy(asc("win_percentage")).head().getString(0)

//Getting their stats
val mostWinsJuniorDf = winnerIsJuniorDf.filter($"winner_name_2"===mostWinsJunior)

val mostWinsJuniorStats = mostWinsJuniorDf.select("winner_aces", "winner_double_faults", 
	"winner_first_serve_return_won_percentage", "winner_second_serve_return_won_percentage", 
	"winner_service_points_won_percentage", "winner_return_points_won_percentage", 
	"winner_break_points_saved","winner_break_points_converted", "winner_tiebreaks_won_2").describe()

val mostLossesJuniorDf = loserIsJuniorDf.filter($"loser_name_2"===lowestWinsJunior)

val leastWinsJuniorStats = mostLossesJuniorDf.select("loser_aces", "loser_double_faults", 
	"loser_first_serve_return_won_percentage", "loser_second_serve_return_won_percentage", 
	"loser_service_points_won_percentage", "loser_return_points_won_percentage", 
	"loser_break_points_saved","loser_break_points_converted", "loser_tiebreaks_won_2").describe()

println(s"Highest Win Percentage Junior Stats: $mostWinsJunior")
mostWinsJuniorStats.show()
println("\n")

println(s"Lowest Win Percentage Junior Stats: $lowestWinsJunior")
leastWinsJuniorStats.show()

//Percentage comparison
val mostWinsJuniorMean = mostWinsJuniorStats.filter($"summary" === "mean").drop("summary").collect()(0).toSeq.map(_.toString.toDouble)
val mostLossesJuniorMean = leastWinsJuniorStats.filter($"summary" === "mean").drop("summary").collect()(0).toSeq.map(_.toString.toDouble)

//Calculating percentage difference between best and worst player for the means of various stats
val compMeanBestvsWorstJunior = mostWinsJuniorMean.zip(mostLossesJuniorMean).map { case (most, least) => 100*((most-least)/((least+most)/2)) }

val cols = Seq("aces", "double faults", "1st serve return won", "2nd serve return_won", 
	"service points won", "return points won", "break points saved", "break points converted", "tiebreaks won")

println("Percentage difference in average stats of top Junior player vs bottom Junior player:\n")
println(cols.zip(compMeanBestvsWorstJunior).map { case (col, value) => s"$col: $value %" }.mkString("\n"))


//--------------------------------Top junior to top 9 junior after him------------------------------
println("\n Analyzing Top Junior vs Next Top 9 Juniors")

val top10PlayersJunior = topJunior.take(10).map(_.getString(0))

val top10PlayersJuniorDf = winnerIsJuniorDf.filter($"winner_name_2".isin(top10PlayersJunior:_*))

val top9playersJunior = top10PlayersJuniorDf.filter($"winner_name_2" !== mostWinsJunior).select("winner_aces", "winner_double_faults", 
	"winner_first_serve_return_won_percentage", "winner_second_serve_return_won_percentage", 
	"winner_service_points_won_percentage", "winner_return_points_won_percentage", 
	"winner_break_points_saved","winner_break_points_converted", "winner_tiebreaks_won_2").describe()

val top9JuniorMean = top9playersJunior.filter($"summary" === "mean").drop("summary").collect()(0).toSeq.map(_.toString.toDouble)

println(s"Highest Win Percentage Junior Stats:")
mostWinsJuniorStats.show()

println(s"Next 9 Top Junior Players Stats:")
top9playersJunior.show()

//Calculating means as percentage
val compMeanT1vT9 = mostWinsJuniorMean.zip(top9JuniorMean).map { case (junior, top9) => 100*((junior-top9)/((top9+junior/2))) }

println("Percentage difference in average stats of top Junior player vs top 9 Junior players:\n")
println(cols.zip(compMeanT1vT9).map { case (col, value) => s"$col: $value %" }.mkString("\n"))


//--------------------------------Bottom junior to 9 better him-------------------------------------
println("\n Analyzing Bottom Junior vs Next 9 Worse Juniors")

val bottom10PlayersJunior = bottomJunior.take(10).map(_.getString(0))

val bottom10PlayersJuniorDf = loserIsJuniorDf.filter($"loser_name_2".isin(bottom10PlayersJunior:_*))

val bottom9playersJunior = bottom10PlayersJuniorDf.filter($"loser_name_2" !== lowestWinsJunior).select("loser_aces", "loser_double_faults", 
	"loser_first_serve_return_won_percentage", "loser_second_serve_return_won_percentage", 
	"loser_service_points_won_percentage", "loser_return_points_won_percentage", 
	"loser_break_points_saved","loser_break_points_converted", "loser_tiebreaks_won_2").describe()

val bottom9JuniorMean = bottom9playersJunior.filter($"summary" === "mean").drop("summary").collect()(0).toSeq.map(_.toString.toDouble)

val compMeanB1vB9 = mostLossesJuniorMean.zip(bottom9JuniorMean).map { case (junior, bottom9) => 100*((junior-bottom9)/((bottom9+junior/2))) }

println(s"Lowest Win Percentage Junior Stats:")
leastWinsJuniorStats.show()

println(s"Next 9 Bottom Junior Players Stats:")
bottom9playersJunior.show()

println("Percentage difference in average stats of bottom Junior player vs next bottom 9 Junior players:\n")
println(cols.zip(compMeanB1vB9).map { case (col, value) => s"$col: $value %" }.mkString("\n"))


//--------------------------------Top 10 juniors vs Bottom 10 Juniors-------------------------------
println("\n Analyzing Top 10 Juniors vs Bottom 10 Juniors")

val top10JuniorStats = top10PlayersJuniorDf.select("winner_ht_3","winner_aces", "winner_double_faults", 
	"winner_first_serve_return_won_percentage", "winner_second_serve_return_won_percentage", 
	"winner_service_points_won_percentage", "winner_return_points_won_percentage", 
	"winner_break_points_saved","winner_break_points_converted", "winner_tiebreaks_won_2").describe()

val top10JuniorMean = top10JuniorStats.filter($"summary" === "mean").drop("summary").collect()(0).toSeq.map(_.toString.toDouble)

val bottom10JuniorStats = bottom10PlayersJuniorDf.select("loser_ht_3","loser_aces", "loser_double_faults", 
	"loser_first_serve_return_won_percentage", "loser_second_serve_return_won_percentage", 
	"loser_service_points_won_percentage", "loser_return_points_won_percentage", 
	"loser_break_points_saved","loser_break_points_converted", "loser_tiebreaks_won_2").describe()

val bottom10JuniorMean = bottom10JuniorStats.filter($"summary" === "mean").drop("summary").collect()(0).toSeq.map(_.toString.toDouble)

val compMeanT10vB10 = top10JuniorMean.zip(bottom10JuniorMean).map { case (top, bottom) => 100*((top-bottom)/((top+bottom/2))) }

println("Top 10 Juniors Stats")
top10JuniorStats.show()

println("Bottom 10 Juniors Stats")
bottom10JuniorStats.show()

val cols2 = Seq("height","aces", "double faults", "service points", "1st In", "1st Won", "2nd Won", "Service Games", "break points saved", "break points faced")

println("Percentage difference in average stats of Top 10 Junior players vs Bottom 10 Junior players:\n")
println(cols2.zip(compMeanT10vB10).map { case (col, value) => s"$col: $value %" }.mkString("\n"))


//*******************************WITHIN SENIORS*********************************

val winnerlsSeniorDf = cleanDf.filter($"winner_age_3">18)

val loserlsSeniorDf = cleanDf.filter($"loser_age_3">18)

val numOfWinsSenior = winnerlsSeniorDf.groupBy("winner_name_2").agg(count("*").alias("wins")).orderBy(desc("wins"))

val numOfLossesSenior = loserlsSeniorDf.groupBy("loser_name_2").agg(count("*").alias("losses")).orderBy(desc("losses"))

val joinedSeniorStats = numOfWinsSenior.join(numOfLossesSenior, numOfWinsSenior("winner_name_2") === numOfLossesSenior("loser_name_2"), "inner").select(numOfWinsSenior("winner_name_2").alias("player_name"), numOfWinsSenior("wins").alias("wins"), numOfLossesSenior("losses").alias("losses"))

val winLossPrecentageSenior = joinedSeniorStats.withColumn("win_percentage", col("wins") / (col("wins")+col("losses")))


//Players with highest win percentage
val topSenior = winLossPrecentageSenior.orderBy(desc("win_percentage"))
println("Seniors with Highest Win Percentage")
topSenior.show()

//Players with lowest win percentage
val bottomSenior = winLossPrecentageSenior.orderBy(asc("win_percentage"))
println("Seniors with Lowest Win Percentage")
bottomSenior.show()

//Highest wins percentage Senior

println("\n Analyzing Top Senior vs Worst Senior")

val mostWinsSenior = winLossPrecentageSenior.orderBy(desc("win_percentage")).head().getString(0)

val lowestWinsSenior = winLossPrecentageSenior.orderBy(asc("win_percentage")).head().getString(0)

//Getting their stats
val mostWinsSeniorDf = winnerlsSeniorDf.filter($"winner_name_2"===mostWinsSenior)

val mostWinsSeniorStats = mostWinsSeniorDf.select("winner_aces", "winner_double_faults", "winner_first_serve_return_won_percentage", "winner_second_serve_return_won_percentage", "winner_service_points_won_percentage", "winner_return_points_won_percentage", "winner_break_points_saved","winner_break_points_converted"
).describe()

val mostLossesSeniorDf = loserlsSeniorDf.filter($"loser_name_2"===lowestWinsSenior)

val leastWinsSeniorStats = mostLossesSeniorDf.select("loser_aces", "loser_double_faults", 
	"loser_first_serve_return_won_percentage", "loser_second_serve_return_won_percentage", 
	"loser_service_points_won_percentage", "loser_return_points_won_percentage", 
	"loser_break_points_saved","loser_break_points_converted", "loser_tiebreaks_won_2").describe()

println(s"Highest Win Percentage Senior Stats: $mostWinsSenior")
mostWinsSeniorStats.show()
println("\n")

println(s"Lowest Win Percentage Senior Stats: $lowestWinsSenior")
leastWinsSeniorStats.show()

//Percentage comparison
val mostWinsSeniorMean = mostWinsSeniorStats.filter($"summary" === "mean").drop("summary").collect()(0).toSeq.map(_.toString.toDouble)
val mostLossesSeniorMean = leastWinsSeniorStats.filter($"summary" === "mean").drop("summary").collect()(0).toSeq.map(_.toString.toDouble)

//Calculating percentage difference between best and worst player for the means of various stats
val compMeanBestvsWorstSenior = mostWinsSeniorMean.zip(mostLossesSeniorMean).map { case (most, least) => 100*((most-least)/((least+most)/2)) }

println("Percentage difference in average stats of top Senior player vs bottom 9 Senior players:\n")
println(cols.zip(compMeanBestvsWorstSenior).map { case (col, value) => s"$col: $value %" }.mkString("\n"))


//--------------------------------Top Senior to top 9 Senior after him------------------------------
println("\n Analyzing Top Senior vs Next Top 9 Seniors")

val top10PlayersSenior = topSenior.take(10).map(_.getString(0))

val top10PlayersSeniorDf = winnerlsSeniorDf.filter($"winner_name_2".isin(top10PlayersSenior:_*))

val top9playersSenior = top10PlayersSeniorDf.filter($"winner_name_2"!== mostWinsSenior).select("winner_aces", "winner_double_faults", 
	"winner_first_serve_return_won_percentage", "winner_second_serve_return_won_percentage", 
	"winner_service_points_won_percentage", "winner_return_points_won_percentage", 
	"winner_break_points_saved","winner_break_points_converted", "winner_tiebreaks_won_2").describe()

val top9SeniorMean = top9playersSenior.filter($"summary" === "mean").drop("summary").collect()(0).toSeq.map(_.toString.toDouble)

println("Top Senior Stats:")
mostWinsSeniorStats.show()
println("Next Top 9 Senior Stats:")
top9playersSenior.show()

//Calculating means as percentage
val compMeanT1vT9S = mostWinsSeniorMean.zip(top9SeniorMean).map { case (senior, top9) => 100*((senior-top9)/((top9+senior/2))) }

println("Percentage difference in average stats of top Senior player vs next top 9 Senior players:\n")
println(cols.zip(compMeanT1vT9S).map { case (col, value) => s"$col: $value %" }.mkString("\n"))


//--------------------------------Bottom Senior to 9 better him-------------------------------------
println("\n Analyzing Bottom Senior vs Next Bottom 9 Seniors (above worst senior)")

val bottom10PlayersSenior = bottomSenior.take(10).map(_.getString(0))

val bottom10PlayersSeniorDf = loserlsSeniorDf.filter($"loser_name_2".isin(bottom10PlayersSenior:_*))

val bottom9playersSenior = bottom10PlayersSeniorDf.filter($"loser_name_2" !== lowestWinsSenior).select("loser_aces", "loser_double_faults", 
	"loser_first_serve_return_won_percentage", "loser_second_serve_return_won_percentage", 
	"loser_service_points_won_percentage", "loser_return_points_won_percentage", 
	"loser_break_points_saved","loser_break_points_converted", "loser_tiebreaks_won_2").describe()

val bottom9SeniorMean = bottom9playersSenior.filter($"summary" === "mean").drop("summary").collect()(0).toSeq.map(_.toString.toDouble)

val compMeanB1vB9S = mostLossesSeniorMean.zip(bottom9SeniorMean).map { case (senior, bottom9) => 100*((senior-bottom9)/((bottom9+senior/2))) }

println("Bottom Senior Stats:")
leastWinsSeniorStats.show()

println("Next Bottom 9 Senior Stats:")
bottom9playersSenior.show()

println("Percentage difference in average stats of bottom Senior player vs next bottom 9 Senior players:\n")
println(cols.zip(compMeanB1vB9S).map { case (col, value) => s"$col: $value %" }.mkString("\n"))


//--------------------------------Top 10 Seniors vs Bottom 10 Seniors-------------------------------
println("\n Analyzing Top 10 Seniors vs Bottom 10 Seniors")

val top10SeniorStats = top10PlayersSeniorDf.select("winner_ht_3","winner_aces", "winner_double_faults", 
	"winner_first_serve_return_won_percentage", "winner_second_serve_return_won_percentage", 
	"winner_service_points_won_percentage", "winner_return_points_won_percentage", 
	"winner_break_points_saved","winner_break_points_converted", "winner_tiebreaks_won_2").describe()

val top10SeniorMean = top10SeniorStats.filter($"summary" === "mean").drop("summary").collect()(0).toSeq.map(_.toString.toDouble)

val bottom10SeniorStats = bottom10PlayersSeniorDf.select("loser_ht_3","loser_aces", "loser_double_faults", 
	"loser_first_serve_return_won_percentage", "loser_second_serve_return_won_percentage", 
	"loser_service_points_won_percentage", "loser_return_points_won_percentage", 
	"loser_break_points_saved","loser_break_points_converted", "loser_tiebreaks_won_2").describe()

val bottom10SeniorMean = bottom10SeniorStats.filter($"summary" === "mean").drop("summary").collect()(0).toSeq.map(_.toString.toDouble)

val compMeanT10vB10S = top10SeniorMean.zip(bottom10SeniorMean).map { case (top, bottom) => 100*((top-bottom)/((top+bottom/2))) }

println("Top 10 Seniors Stats")
top10SeniorStats.show()
println("Bottom 10 Seniors Stats")
bottom10SeniorStats.show()

println("Percentage difference in average stats of Top 10 Senior players vs Bottom 10 Senior players:\n")
println(cols2.zip(compMeanT10vB10S).map { case (col, value) => s"$col: $value %" }.mkString("\n"))


//********************************JUNIORS VS SENIORS******************************************

println("----------------------------------------------------")
println("JUNIORS VS SENIORS COMPARISON")
println("----------------------------------------------------")
//Doing top to top player analysis

println("\n Comparing Top 10 Seniors vs Top 10 Juniors Win Percentage")

//Doing top 10 to top 10 player junior vs senior analysis 
val meanWinPercentageS = topSenior.limit(10).agg(mean("win_percentage").as("mean_win_percentage")).select("mean_win_percentage").first().getDouble(0)
val meanWinPercentageJ = topJunior.limit(10).agg(mean("win_percentage").as("mean_win_percentage")).select("mean_win_percentage").first().getDouble(0)
val diffInMeans = meanWinPercentageS-meanWinPercentageJ

//Compare their win percentages 
println(s"Mean win percentages for top seniors: $meanWinPercentageS")
println(s"Mean win percentages for top juniors: $meanWinPercentageJ")
println(s"Difference in means: $diffInMeans")


println("\n Analyzing if there is a Correlation Between win percentage and ATP rank")

//See if there is a correlation between win percentage and ATP rank

//Choosing highest rank of the player 

// Define a window specification partitioned by winner_name and ordered by winner_rank
val windowSpec = Window.partitionBy("winner_name_2").orderBy(col("winner_rank_3").asc)

// Add a new column containing the highest rank for each winner
//Seniors
val winnersHighestRank = winnerlsSeniorDf.withColumn("winners_highest_rank", first("winner_rank_3").over(windowSpec)).select("winner_name_2", "winners_highest_rank").distinct()

val mergedWinPercentATP = topSenior.join(winnersHighestRank, topSenior("player_name") === winnersHighestRank("winner_name_2"), "inner")

//Juniors
val winnersHighestRankJ = winnerIsJuniorDf.withColumn("winners_highest_rank", first("winner_rank_3").over(windowSpec)).select("winner_name_2", "winners_highest_rank").distinct()

val mergedWinPercentATPJ = topJunior.join(winnersHighestRankJ, topJunior("player_name") === winnersHighestRankJ("winner_name_2"), "inner")


println("Ranks vs Win Percentages (for top 10)")

println("Seniors:")
mergedWinPercentATP.select("player_name", "win_percentage", "winners_highest_rank").orderBy(desc("win_percentage")).show()

println("Juniors:")
mergedWinPercentATPJ.select("player_name", "win_percentage", "winners_highest_rank").orderBy(desc("win_percentage")).show()


val windowSpecloser = Window.partitionBy("loser_name_2").orderBy(col("loser_rank_3").asc)

// Add a new column containing the highest rank for each winner
val losersHighestRank = loserlsSeniorDf.withColumn("losers_highest_rank", first("loser_rank_3").over(windowSpecloser)).select("loser_name_2", "losers_highest_rank").distinct()

val mergedWinPercentATPloser = bottomSenior.join(losersHighestRank, bottomSenior("player_name") === losersHighestRank("loser_name_2"), "inner")

val losersHighestRankJ = loserIsJuniorDf.withColumn("losers_highest_rank", first("loser_rank_3").over(windowSpecloser)).select("loser_name_2", "losers_highest_rank").distinct()

val mergedWinPercentATPloserJ = bottomJunior.join(losersHighestRankJ, bottomJunior("player_name") === losersHighestRankJ("loser_name_2"), "inner")


println("Ranks vs Win Percentages (for bottom 10)")

println("Seniors:")
mergedWinPercentATPloser.select("player_name", "win_percentage", "losers_highest_rank").orderBy(asc("win_percentage")).show()

println("Juniors:")
mergedWinPercentATPloserJ.select("player_name", "win_percentage", "losers_highest_rank").orderBy(asc("win_percentage")).show()

//Can see general trend for seniors but not for juniors (maybe junior ranking doesn't matter as much as junior win percentage)

//Compare match stats
println("\n Comparing Top 10 Seniors vs Top 10 Juniors Match Stats")

val compMeanT10SvT10J = top10SeniorMean.zip(top10JuniorMean).map { case (top, bottom) => (top-bottom) }

println("Top 10 Senior Stats:")
top10SeniorStats.show()
println("Top 10 Junior Stats:")
top10JuniorStats.show()

println("Difference in average stats of Top 10 Senior players vs Top 10 Junior players:\n")
println(cols2.zip(compMeanT10SvT10J).map { case (col, value) => s"$col: $value" }.mkString("\n"))

//Doing bottom to bottom player analysis

//Doing bottom 10 to bottom 10 player junior vs senior analysis 

println("\n Comparing Bottom 10 Seniors vs Bottom 10 Juniors Win Percentage")

val meanWinPercentageSbottom = bottomSenior.limit(10).agg(mean("win_percentage").as("mean_win_percentage")).select("mean_win_percentage").first().getDouble(0)
val meanWinPercentageJbottom = bottomJunior.limit(10).agg(mean("win_percentage").as("mean_win_percentage")).select("mean_win_percentage").first().getDouble(0)
val diffInMeansbottom = meanWinPercentageS-meanWinPercentageJ

//Compare their win percentages 
println(s"Mean win percentages for bottom seniors: $meanWinPercentageSbottom")
println(s"Mean win percentages for bottom juniors: $meanWinPercentageJbottom")
println(s"Difference in means: $diffInMeansbottom")

//Compare match stats
println("\n Comparing Bottom 10 Seniors vs Bottom 10 Juniors Match Stats")

val compMeanB10SvB10J = bottom10SeniorMean.zip(bottom10JuniorMean).map { case (top, bottom) => (top-bottom) }

println("Bottom 10 Senior Stats:")
bottom10SeniorStats.show()
println("Bottom 10 Junior Stats:")
bottom10JuniorStats.show()

println("Difference in average stats of Bottom 10 Senior players vs Bottom 10 Junior players:\n")
println(cols2.zip(compMeanB10SvB10J).map { case (col, value) => s"$col: $value" }.mkString("\n"))


//*********************************QUALITATIVE FACTORS ANALYSIS*************************************

println("----------------------------------------------------")
println("QUALITATIVE FACTOR ANALYSIS: SURFACE")
println("----------------------------------------------------")

//Compare surfaces they mainly win on (wins on surface) and organize that based on win_percentage

val winsBySurfaceSenior = winnerlsSeniorDf.groupBy("winner_name_2","surface_3").agg(count("*").alias("wins_on_surface")).orderBy(desc("wins_on_surface"))

val winsBySurfaceJunior = winnerIsJuniorDf.groupBy("winner_name_2","surface_3").agg(count("*").alias("wins_on_surface")).orderBy(desc("wins_on_surface"))

val surfaceMergeS = winsBySurfaceSenior.join(topSenior, topSenior("player_name") === winsBySurfaceSenior("winner_name_2"), "inner")

val surfaceMergeJ = winsBySurfaceJunior.join(topJunior, topJunior("player_name") === winsBySurfaceJunior("winner_name_2"), "inner")

//Found pattern
println("Senior Player Wins by Surface:")
surfaceMergeS.select("player_name", "surface_3", "wins_on_surface", "win_percentage").orderBy(desc("win_percentage")).show()
println("Junior Player Wins by Surface:")
surfaceMergeJ.select("player_name", "surface_3", "wins_on_surface", "win_percentage").orderBy(desc("win_percentage")).show()


//#########################################ADDITIONAL ANALYSIS#########################################

println("-----------------------------------------------------------------------------------------------")
println("How often does one see a player win a match despite winning less points than their opponent?")
println("-----------------------------------------------------------------------------------------------")

//How often does one see a player win a match despite winning less points than their opponent?
//Due to its unique scoring system, in tennis it is possible to win a match with fewer points or fewer games than the opponent
//Seeing how often that happens overall

val winnerLessPointsdf = cleanDf.filter($"winner_total_points_won" < $"loser_total_points_won")

val winnerCountLessPoints = winnerLessPointsdf.groupBy("winner_name_2").agg(count("*").alias("matches_won_with_less_points")).orderBy(desc("matches_won_with_less_points"))

println("Overall -- Players who win despite scoring less points frequency chart:")
winnerCountLessPoints.show()

//How much such types of matches happen for juniors
val winnerLessPointsJuniordf = cleanDf.filter($"winner_total_points_won" < $"loser_total_points_won" && $"winner_age_3"<= 18)

val winnerCountLessPointsJ = winnerLessPointsJuniordf.groupBy("winner_name_2").agg(count("*").alias("matches_won_with_less_points")).orderBy(desc("matches_won_with_less_points"))

val mergedwithPercentJCount = winnerCountLessPointsJ.join(topJunior, topJunior("player_name") === winnerCountLessPointsJ("winner_name_2"), "inner")


println("Juniors -- Players who win despite scoring less points frequency chart:")
winnerCountLessPointsJ.show()

//How much such type of matches happen for seniors 

val winnerLessPointsSeniordf = cleanDf.filter($"winner_total_points_won" < $"loser_total_points_won" && $"winner_age_3">18)

val winnerCountLessPointsS = winnerLessPointsJuniordf.groupBy("winner_name_2").agg(count("*").alias("matches_won_with_less_points")).orderBy(desc("matches_won_with_less_points"))

val mergedwithPercentSCount = winnerCountLessPointsS.join(topSenior, topSenior("player_name") === winnerCountLessPointsS("winner_name_2"), "inner")

println("Seniors -- Players who win despite scoring less points frequency chart:")
winnerCountLessPointsS.show()


//Top to top Senior
println("Top Seniors to Top Juniors Such Cases Where Winner Scores Less Points")
mergedwithPercentSCount.select("player_name", "win_percentage","matches_won_with_less_points").orderBy(desc("win_percentage")).show()
mergedwithPercentJCount.select("player_name", "win_percentage","matches_won_with_less_points").orderBy(desc("win_percentage")).show()


//Bottom to bottom
println("Bottom Seniors to Bottom Juniors Such Cases Where Winner Scores Less Points")
mergedwithPercentSCount.select("player_name", "win_percentage","matches_won_with_less_points").orderBy(asc("win_percentage")).show()
mergedwithPercentJCount.select("player_name", "win_percentage","matches_won_with_less_points").orderBy(asc("win_percentage")).show()


//Implication: Matches can not be close, yet be very close.




