#!/bin/bash

source ./config.sh

NUMBER_OF_EXPERIMENT_RUNS=10
NUMBER_OF_WARMUP_RUNS=5

function submit_task {
    /spark/bin/spark-submit --driver-memory 96g --master spark://`hostname`:7077 --executor-memory 96G \
                            --class de.unikl.cs.dbis.waves.testjobs.$1 waves.jar \
                            inputPath=$SOURCE wavesPath=$TARGET inputSchemaPath=$SCHEMA \
                            useColumnSplits=false cleanWavesPath=true numPartitions=150 sampler=init ${@:2}
}

# # Warm up the Cache
# for N in $(seq 1 1 $NUMBER_OF_WARMUP_RUNS)
# do
#     submit_task StoreSchema
# done

# Run experiments
for N in $(seq 1 1 $NUMBER_OF_EXPERIMENT_RUNS)
do
    for count in $(seq 2000 2000 26000) # Twitter
    #for count in $(seq 2000 2000 14000) # GitHub?
    do
        submit_task split.ModelGini perPartition=$count
        submit_task split.ModelGiniWithSort perPartition=$count
    done
done
