#!/bin/bash

NUMBER_OF_EXPERIMENT_RUNS=10
NUMBER_OF_WARMUP_RUNS=5

SOURCE="file:///cluster-share/benchmarks/json/twitter/109g_multiple"
TARGET="hdfs://namenode:9000/benchmark"
SCHEMA="./schemas/twitter.json"

function submit_task {
    /spark/bin/spark-submit --driver-memory 96g --master spark://`hostname`:7077 --executor-memory 96G \
                            --class de.unikl.cs.dbis.waves.testjobs.$1 waves.jar \
                            inputPath=$SOURCE wavesPath=$TARGET inputSchemaPath=$SCHEMA \
                            useColumnSplits=false cleanWavesPath=true ${@:2}
}

# # Warm up the Cache
# for N in $(seq 1 1 $NUMBER_OF_WARMUP_RUNS)
# do
#     submit_task StoreSchema
# done

# Run experiments
for N in $(seq 1 1 $NUMBER_OF_EXPERIMENT_RUNS)
do
    for partitions in 50 150 250
    do
        for min in $(seq 1 1 9)
        do
            if [[ $min == 5 ]]; then 
                continue # already part of benchmark.sh
            fi
            submit_task split.ModelGini numPartitions=$partitions relativeMinSize=0.$min
            submit_task split.ModelGiniWithSort numPartitions=$partitions relativeMinSize=0.$min
        done
    done
done
