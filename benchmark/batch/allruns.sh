#!/bin/bash

source ./config.sh

function submit_task {
    /spark/bin/spark-submit --driver-memory 96g --master spark://`hostname`:7077 --executor-memory 96G \
                            --class de.unikl.cs.dbis.waves.testjobs.$1 waves.jar \
                            inputPath=$SOURCE wavesPath=$TARGET inputSchemaPath=$SCHEMA \
                            useColumnSplits=false cleanWavesPath=true ${@:2}
}

# Run experiments
for max in 250 150 50
do
    for mode in ModelGini ModelGiniWithSort LexicographicMono LexicographicPartitionwise
    do
        for min in 0.5 #0.1 0.5 0.9
        do
            submit_task split.$mode numPartitions=$max relativeMinSize=$min
            ./queries.sh
            mv log/ query_logs/$mode-$max-$min
            mkdir log
        done
        # submit_task split.$mode numPartitions=$max sampler=init perPartition=2000
        # ./queries.sh
        # mv log/ query_logs/$mode-$max-init
        # mkdir log
    done
done
