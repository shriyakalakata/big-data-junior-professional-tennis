
//Reading all the .csv files from the directory 
val directory = "hw6files/*.csv"

val df = spark.read.option("header", "true").csv(directory)

df.show(5)

//Count the number of records.
println("Number of Records: "+df.count())

//Map the records to a key and a value and count the number of records using 
//map() function. (this is to count the unique rows in the dataset)

//Reading whole files, because each csv contains the header, so we must get rid of that
//by using flatmap later on to split the contents and only keep the data lines

val lines = sc.wholeTextFiles(directory)
  .flatMap { case (_, content) =>
    // Split the content of each file by newline character
    val lines = content.split("\n")
    
    // Filter out the header line if it exists
    if (lines.nonEmpty) lines.tail else lines
  }

//val lines: RDD[String] = df.rdd.map(row => row.mkString(","))

//This line maps each line s to a tuple (s, 1), effectively converting each line into a key-value pair where the key is the line itself and the value is 1.
val pairs = lines.map(line => (line, 1)) 

//This line performs a reduction operation by key. It takes the key-value pairs from the previous step and combines the values for each key using addition. So, it effectively counts the occurrences of each unique line in the RDD.
val counts = pairs.reduceByKey((a, b) => a + b)

// Count the number of unique lines
val uniqueLinesCount = counts.count()
    
// Print the count
println("Number of unique lines:"+ counts.count())

//Find the distinct values in each column (columns you are using for your analytic).

//A list of columns we're interested in
val columnNames = List("tourney_name", "surface", "draw_size", "tourney_level", "winner_seed", "winner_entry", "winner_name", "winner_hand",
    "winner_ht", "winner_ioc", "winner_age", "loser_seed", "loser_entry",
    "loser_name", "loser_hand", "loser_ht", "loser_ioc", "loser_age", "score", "best_of",
    "round", "minutes", "w_ace", "w_df", "w_svpt", "w_1stIn", "w_1stWon", "w_2ndWon",
    "w_SvGms", "w_bpSaved", "w_bpFaced", "l_ace", "l_df", "l_svpt", "l_1stIn", "l_1stWon",
    "l_2ndWon", "l_SvGms", "l_bpSaved", "l_bpFaced", "winner_rank", "winner_rank_points",
    "loser_rank", "loser_rank_points")

// Iterate over each column and find distinct values
for (col <- columnNames) {
  println(s"Distinct values for column '$col':")
  df.select(col).distinct.show(false) // Show distinct values without truncating
  println(s"Number of distinct values for column '$col': "+ df.select(col).distinct.count())
}

