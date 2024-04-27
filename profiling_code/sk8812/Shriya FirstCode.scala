import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

val fileName = "tennis_data_cleaned_hw6/part-00000-051cebbf-b15c-4228-a2ca-17d9bb8d0485-c000.csv"
val df_tennis  = spark.read.option("header", "false").csv(fileName)

// Convert columns to integer type
val dfWithIntegers = df_tennis.select(df_tennis.columns.map(colName => {
     |   try {
     |     col(colName).cast(IntegerType).alias(colName)
     |   } catch {
     |     case _: Throwable => col(colName)
     |   }
     | }): _*)

// Find all the integer columns
val integerColumns = dfWithIntegers.dtypes.filter{ case (_, dataType) =>
     |   dataType == "IntegerType"
     | }.map(_._1)

// replace nan w 0
val filledDataFrame = dfWithIntegers.na.fill(0)

// print the median, mode, and mean for all integer columns
integerColumns.foreach { column =>
     |   val meanValue = filledDataFrame.select(mean(col(column))).as[Double].first()
     |   val modeValue = filledDataFrame.groupBy(column).count().orderBy(col("count").desc).select(column).limit(1).first()(0)
     |   val medianValue = filledDataFrame.stat.approxQuantile(column, Array(0.5), 0.001).head
     | 
     |   println(s"Column: $column")
     |   println(s"Mean: $meanValue, Mode: $modeValue, Median: $medianValue")
     |   println("--------------------")
     | }

// find the standard deviation for a specific column
val stdDevValue = dfWithIntegers.select(stddev(col("_c56"))).as[Double].first()

// make all the columns capitalized 
val df_tennisCapitalized = df_tennis.columns.foldLeft(df_tennis) { (acc, colName) =>
     |   acc.withColumn(colName, upper(col(colName)))
     | }

// create a new binary column based on the column "_c6". if the value is greater than 5, then the new column "high aces"  is 1. or else it's 0
val condition = col("_c6") > 5
val df_with_high_aces = filledDataFrame.withColumn("high aces", when(condition, 1).otherwise(0))