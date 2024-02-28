#!/bin/bash

source ./config.sh

NUMBER_OF_EXPERIMENT_RUNS=10

function submit_task {
    /spark/bin/spark-submit --driver-memory 96g --master spark://`hostname`:7077 --executor-memory 96G \
                            --class de.unikl.cs.dbis.waves.testjobs.$1 waves.jar \
                            inputPath=$SOURCE wavesPath=$TARGET inputSchemaPath=$SCHEMA ${@:2}
}

# Run experiments
for N in $(seq 1 1 $NUMBER_OF_EXPERIMENT_RUNS)
do
    submit_task query.BetzeTwitter useWaves=true
    # submit_task query.BetzeGithub useWaves=true
done
for N in $(seq 1 1 $NUMBER_OF_EXPERIMENT_RUNS)
do
    submit_task query.BetzeTwitter useWaves=false
    # submit_task query.BetzeGithub useWaves=false
done