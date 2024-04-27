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

## Contributors
* Shriya Kalakata
* Manjiri Bhandarwar