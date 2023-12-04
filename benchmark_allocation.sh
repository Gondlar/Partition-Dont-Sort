#!/bin/bash

partitions=150

SOURCE="file:///cluster-share/benchmarks/json/twitter/109g_multiple"
TARGET="hdfs://namenode:9000/benchmark"
SCHEMA="./schemas/twitter.json"

function submit_task {
    /spark/bin/spark-submit --driver-memory 96g --master spark://`hostname`:7077 --executor-memory 96G \
                            --class de.unikl.cs.dbis.waves.testjobs.$1 waves.jar \
                            inputPath=$SOURCE wavesPath=$TARGET inputSchemaPath=$SCHEMA \
                            useColumnSplits=false cleanWavesPath=true ${@:2}
}

# Run experiments

submit_task split.ModelGini numPartitions=$partitions
submit_task EvaluateAllocation reportName=allocation/gini.csv
submit_task split.ModelGiniWithSort numPartitions=$partitions
submit_task EvaluateAllocation reportName=allocation/sortedGini.csv
submit_task split.LexicographicMono numPartitions=$partitions
submit_task EvaluateAllocation reportName=allocation/mono.csv
submit_task split.LexicographicPartitionwise numPartitions=$partitions
submit_task EvaluateAllocation reportName=allocation/partitioned.csv