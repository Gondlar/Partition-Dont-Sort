#!/bin/bash

source ./config.sh

NUMBER_OF_EXPERIMENT_RUNS=10
NUMBER_OF_WARMUP_RUNS=5

function submit_task {
    /spark/bin/spark-submit --driver-memory 96g --master spark://`hostname`:7077 --executor-memory 96G \
                            --class de.unikl.cs.dbis.waves.testjobs.$1 waves.jar \
                            inputPath=$SOURCE wavesPath=$TARGET inputSchemaPath=$SCHEMA \
                            cleanWavesPath=true ${@:2}
}

# Warm up the Cache
for N in $(seq 1 1 $NUMBER_OF_WARMUP_RUNS)
do
    submit_task StoreSchema
done

# Run experiments
for N in $(seq 1 1 $NUMBER_OF_EXPERIMENT_RUNS)
do
    submit_task split.Plain
done