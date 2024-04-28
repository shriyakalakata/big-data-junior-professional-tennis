# Big Data Project: Junior and Professional Tennis Data Analysis

## Raw Data
We're using three different datasets in this analysis:
* **ATP World Tour Tennis Data and Data from Tennis Abstract**
    * Source: https://github.com/JeffSackmann/tennis_atp/tree/master
    * Where to find in this repo: `data_ingest/tennis_atp_master/`
* **ATP World Tour Tennis Data (Match Stats)**
    * Source: https://github.com/serve-and-volley/atp-world-tour-tennis-data/tree/master/csv/3_match_stats
    * Where to find in this repo: `data_ingest/match_stats/`
* **ATP World Tour Tennis Data (Match Scores)**
    * Source: https://github.com/serve-and-volley/atp-world-tour-tennis-data/tree/master/csv/2_match_scores
    * Where to find in this repo: `data_ingest/match_scores/`

## Building and Running Code

### Running the Code on Dataproc/HDFS

1. **Setup Dataproc Cluster**: Ensure you have access to a Google Cloud Platform (GCP) account and have set up a Dataproc cluster.
2. **Upload Data to HDFS**: Transfer your input data to the Hadoop Distributed File System (HDFS) on your Dataproc cluster. (Using the data_ingest.txt instructions)
3. **Open Spark Shell**: Access the Spark shell on your Dataproc cluster by running the following command:
```
spark-shell --deploy-mode client
```

### Important Note:
The paths in our code files (for getting input data and outputting and saving a modified dataset into HDFS) might be different from what you need to put. For example, we have ``` val file = "tennis_merged_final/part-00000-baea738d-95aa-4f4b-b952-02b19f4e3fbe-c000.csv"``` and ```dfWithoutDuplicates.coalesce(1).write.option("header",true).csv("tennis_merged_final_clean/")```

### Data Cleaning, Profiling, and Merging
1. **Data cleaning and profiling for `match_stats`**: 
    * Use  `data_ingest/match_stats/` as your input data
         Note: `data_ingest/match_stats/` is not the hdfs directory name. That would be `input_final/match_stats/`.
    * `etl_code/sk8812/match_stats_cleaning.scala`contains the code for this data cleaning.
    * More commands in `data_ingest/data_ingest.txt`
2. **Data cleaning and profiling for `match_scores`**: 
    * Use  `data_ingest/match_scores/` as your input data
    * `etl_code/match_scores_cleaning.scala`contains the code for this data cleaning.
    * More commands in `data_ingest/data_ingest.txt`
3. **Merging `match_stats` and `match_scores`**: 
    * `merge_first.scala` contains the data for this merging and cleaning
4. **Data cleaning and profiling for `tennis_atp_master`**: 
    * Use  `data_ingest/tennis_atp_master/` as your input data
    * `etl_code/mb8070/tennis_atp_master_cleaning.scala`contains the code for this data cleaning.
    * More commands in `data_ingest/data_ingest.txt`
5. **Merging `tennis_atp_master`  with the merged and cleaned dataset of `match_stats` and `match_scores` to get the final dataset**: 
    * `merging_all.scala` contains the code for the merging
6. **Cleaning and profiling for the final dataset**:
    * `final_dataset_cleaning.scala` contains the code for cleaning the final dataset.

### Checklist of directories after this step
You should have the following directories in your HDFS
- input_final/
- tennis_merged_final_clean/ (which now contains the merged and clean dataset we will be using for the analytic)

### Data Analysis
**Method 1: [PREFERRED]**
1. If you are doing this step, refresh your dataproc website.
2. Upload finalAnalyticManjiriShriya.scala (which is in the ana_code directory) file to dataproc
3. Run the command below on HDFS to
```
spark-shell --deploy-mode client -i finalAnalyticManjiriShriya.scala
```
This will give a cleaner analysis output.

**Method 2:**
1. If you are already in the spark shell from the previous parts, run each command from finalAnalyticManjiriShriya.scala one by one in the shell.

## Directory Structure
* data_ingest/
    * data_ingest.txt
    * tennis_atp_master/
    * match_stats/
    * match_scores/
* data_other/
    * final_dataset.csv
* etl_code/
    * final_dataset_cleaning.scala
    * match_scores_cleaning.scala
    * merge_first.scala
    * merging_all.scala
    * sk8812/
        * Shriya Clean.scala
        * match_stats_cleaning.scala
    * mb8070/
        * Manjiri Clean.scala
        * tennis_atp_master_cleaning.scala
* profiling_code/
    * sk8812/
        * Shriya CountRecs.scala
        * Shriya FirstCode.scala
    * mb8070/
        * Manjiri CountRecs.scala
        * Manjiri FirstCode.scala
* ana_code/
    * finalAnalyticManjiriShriya.scala
* screenshots/
    * Main Analytics Ouput 
    * Analytics Code Step by Step 
    * final_dataset_cleaning_screenshots
    * match_scores_cleaning_screenshots
    * match_stats_cleaning_screenshots
    * merge_all_datasets_screenshots
    * merge_first_screenshots
    * tennis_atp_master_cleaning_screenshots
* README.md

## Notes
Files Shriya Clean.scala, Shriya CountRecs.scala, Shriya FirstCode.scala, Manjiri Clean.scala, Manjiri CountRecs.scala, Manjiri FirstCode.scala are used to analyze individual datasets and are provided to show how code was repurposed to form a cleaned merged dataset. The directories used to load the input data in these are from our hw6 directories, to which access is already provided.

Main Analytics Ouput and Analytics Code Step by Step essential have the same output, its main analytics is when the whole scala file is run at once, and step by step is screenshots of when commands are run on spark shell one by one.

## Contributors
* Shriya Kalakata
* Manjiri Bhandarwar
