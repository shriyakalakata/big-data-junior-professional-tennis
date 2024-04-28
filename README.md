# Big Data Project: Junior and Professional Tennis Data Analysis

## Raw Data
We're using three different datasets in this analysis:
* **ATP World Tour Tennis Data and Data from Tennis Abstract**
    * Source: https://github.com/JeffSackmann/tennis_atp/tree/master
    * Where to find in this repo: `data_ingest/tennis_atp_master`
* **ATP World Tour Tennis Data (Match Stats)**
    * Source: https://github.com/serve-and-volley/atp-world-tour-tennis-data/tree/master/csv/3_match_stats
    * Where to find in this repo: `data_ingest/match_stats`
* **ATP World Tour Tennis Data (Match Scores)**
    * Source: https://github.com/serve-and-volley/atp-world-tour-tennis-data/tree/master/csv/2_match_scores
    * Where to find in this repo: `data_ingest/match_scores`

## Building and Running Code

### Running the Code on Dataproc/HDFS

1. **Setup Dataproc Cluster**: Ensure you have access to a Google Cloud Platform (GCP) account and have set up a Dataproc cluster.
2. **Upload Data to HDFS**: Transfer your input data to the Hadoop Distributed File System (HDFS) on your Dataproc cluster.
3. **Open Spark Shell**: Access the Spark shell on your Dataproc cluster by running the following command:
```
spark-shell --deploy-mode client
```

## Directory Structure
* data_ingest/
    * data_ingest.txt
    * tennis_atp_master/
    * match_stats/
    * match_scores/
* data_other/
    * TO-DO
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
    * TO-DO
* screenshots/
    * final_dataset_cleaning_screenshots
    * match_scores_cleaning_screenshots
    * match_stats_cleaning_screenshots
    * merge_all_datasets_screenshots
    * merge_first_screenshots
    * tennis_atp_master_cleaning_screenshots
* README.md

## Contributors
* Shriya Kalakata
* Manjiri Bhandarwar