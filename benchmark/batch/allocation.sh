#!/bin/bash

source ./config.sh

partitions=150

function submit_task {
    /spark/bin/spark-submit --driver-memory 96g --master spark://`hostname`:7077 --executor-memory 96G \
                            --class de.unikl.cs.dbis.waves.testjobs.$1 waves.jar \
                            inputPath=$SOURCE wavesPath=$TARGET inputSchemaPath=$SCHEMA \
                            useColumnSplits=false ${@:2}
}

# Run experiments

submit_task split.ModelGini cleanWavesPath=true numPartitions=$partitions
submit_task EvaluateAllocation cleanWavesPath=false reportName=allocation/gini.csv
submit_task split.ModelGiniWithSort cleanWavesPath=true numPartitions=$partitions
submit_task EvaluateAllocation cleanWavesPath=false reportName=allocation/sortedGini.csv
submit_task split.LexicographicMono cleanWavesPath=true numPartitions=$partitions
submit_task EvaluateAllocation cleanWavesPath=false reportName=allocation/mono.csv
submit_task split.LexicographicPartitionwise cleanWavesPath=true numPartitions=$partitions
submit_task EvaluateAllocation cleanWavesPath=false reportName=allocation/partitioned.csv