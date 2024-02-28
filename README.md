This is the Code Availability Package for the paper "Partition, Don't Sort! Compression Boosters for Cloud Data Ingestion Pipelines".

# Build and Run

This project is written in the Scala programming language and employs SBT for build management.
To build the project, run

```bash
sbt package
```

To submit a job to Spark, run the following Spark command or use one of the provided benchmark scripts (see below):

```bash
spark-submit --master spark://hostname:7077 --class de.unikl.cs.dbis.waves.testjobs.$1 waves.jar \
             inputPath=/path/to/json/input wavesPath=hdfs://namenode/target/location inputSchemaPath=/path/to/schema.json
```

# Code Overview

This repository contains a general framework for managing ingestion pipelines including code for previous publications.
The most relevant implementations for this paper are as follows:

 * [The Fingerprint Set](src/main/scala/de/unikl/cs/dbis/waves/util/TotalFingerprint.scala)
 * [Gathering the Fingerprint Set](src/main/scala/de/unikl/cs/dbis/waves/pipeline/util/CalculateTotalFingerprint.scala)
 * [The Adapted Hunt's Algorithm](src/main/scala/de/unikl/cs/dbis/waves/pipeline/split/ModelGini.scala)

The Spark Jobs run for evaluation are the following:

 * [Gini approach](src/main/scala/de/unikl/cs/dbis/waves/testjobs/split/ModelGini.scala)
 * [Gini+ approach](src/main/scala/de/unikl/cs/dbis/waves/testjobs/split/ModelGiniWithSort.scala)
 * [Global approach](src/main/scala/de/unikl/cs/dbis/waves/testjobs/split/LexicographicMono.scala)
 * [Builtin approach](src/main/scala/de/unikl/cs/dbis/waves/testjobs/split/LexicographicPartitionwise.scala)
 * [Schema Extraction](src/main/scala/de/unikl/cs/dbis/waves/testjobs/StoreSchema.scala) (used for warming up the caches)
 * [Allocation Measurement](src/main/scala/de/unikl/cs/dbis/waves/testjobs/EvaluateAllocation.scala)
 * [BETZE Queries Twitter](src/main/scala/de/unikl/cs/dbis/waves/testjobs/query/BetzeTwitter.scala)
 * [BETZE Queries GitHub](src/main/scala/de/unikl/cs/dbis/waves/testjobs/query/BetzeGithub.scala)

# Benchmark

For ease of use, we also provide [scripts](benchmark) that gather log files for the experimental results we present in the paper.
Before running, adapt [config.sh](benchmark/batch/config.sh) to your cluster configuration.

 * [runtime.sh](benchmark/batch/runtume.sh) collects ingestion times and compressed sizes for Figures 3 - 5
 * [minSize.sh](benchmark/batch/minSize.sh) collects ingestion times and compressed sizes for Figures 6 and 7
 * [initSample.sh](benchmark/batch/initSample.sh) collects ingestion times and compressed sizes Figure 8
 * [allocation.sh](benchmark/batch/allocation.sh) collects Sizes per partition for Figure 9
 * [baseline.sh](benchmark/batch/baseline.sh) determines the ingestion times and compressed sizes without boosting the compression as stated in Table 3. They are required to compute boost factor and slowdown.
 * [allruns.sh](benchmark/batch/allruns.sh) measures the query times for Figure 10

To parse the logfiles and convert them into CSV tables, we also provide two python scripts, one for [ingestion logs](benchmark/parser/logs.py) and one for [query logs](benchmark/parser/query_logs.py)
