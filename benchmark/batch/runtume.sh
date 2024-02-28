#!/bin/bash

source ./config.sh

NUMBER_OF_EXPERIMENT_RUNS=10
NUMBER_OF_WARMUP_RUNS=5

function submit_task {
    /spark/bin/spark-submit --driver-memory 96g --master spark://`hostname`:7077 --executor-memory 96G \
                            --class de.unikl.cs.dbis.waves.testjobs.$1 waves.jar \
                            inputPath=$SOURCE wavesPath=$TARGET inputSchemaPath=$SCHEMA \
                            useColumnSplits=false cleanWavesPath=true ${@:2}
}

# Warm up the Cache
for N in $(seq 1 1 $NUMBER_OF_WARMUP_RUNS)
do
    submit_task StoreSchema
done

# Run experiments
for N in $(seq 1 1 $NUMBER_OF_EXPERIMENT_RUNS)
do
    for partitions in $(seq 10 10 250)
    do
        submit_task split.ModelGini numPartitions=$partitions
        submit_task split.ModelGiniWithSort numPartitions=$partitions
        submit_task split.LexicographicMono numPartitions=$partitions
        submit_task split.LexicographicPartitionwise numPartitions=$partitions
    done
done
