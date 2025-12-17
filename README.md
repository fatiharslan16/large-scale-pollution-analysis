# Big Data Project with Java and Hadoop
Transformed 7M daily records (3.3 GB) into 20 years of state/national averages in under 2 minutes using optimized Hadoop (YARN).

## Technology Stack
- **Language:** Java SE 25
- **Storage:** HDFS 
- **Computing Framework:** MapReduce + Hadoop
- **Java APIs:** Mapper, Reducer, Context, TextInputFormat, FileInputFormat, and FileOutputFormat

## Hadoop Job Flow
- **Job 1 (DataPrep.java):** Data preparing. Parses raw daily records, extracts stateCode and dailyMean, and pairs them with the year from the file name.
- **Job 2 (AnnualAverage.java):** State Aggregation. Groups the prepared daily data by stateCode_year to compute the annual average per state.
- **Job 3 (USAnnual.java):** National Aggregation. Group the sttate annual averages by year and outputs the final U.S. national annual average.

## Dataset
- **Source:** United States Environmental Protection Agency Data [https://aqs.epa.gov/aqsweb/airdata/download_files.html#Daily]
