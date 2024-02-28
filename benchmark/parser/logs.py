#!/usr/bin/env python3

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import os
import time

#logdir = "/home/patrick/Promotion/Publications/partition-dont-sort/results/twitter-sample-init2/log"
logdir = "/home/patrick/log_fixed"

def quotes(s: str):
    """
    Remove quote marks from a string if they are present
    :param s: the string to strip
    :return: the stripped string
    """
    if s[0] == '\'' and s[-1] == '\'':
        return s[1:-1]
    return s

def read_run(path):
    """
    Read a logfile into a dict
    :param path: the path to the logfile
    :return: the dict
    """
    data = np.genfromtxt( path
                        , delimiter=','
                        , dtype=None
                        , encoding="utf-8"
                        , converters= { 1: lambda s: quotes(s)
                                      , 2: lambda s: quotes(s)
                                      }
                        )
    run = dict()
    for line in data:
        run[line[1]] = (line[0], line[2])
    return run

def parse_gather_data(run):
    type = run.get("split-start")[1]
    if type == "de.unikl.cs.dbis.waves.testjobs.split.ModelGini$" or type == "de.unikl.cs.dbis.waves.testjobs.split.ModelGiniWithSort$":
        return run.get("end-CalculateTotalFingerprint")[0] - run.get("start-CalculateTotalFingerprint")[0]
    if type == "de.unikl.cs.dbis.waves.testjobs.split.LexicographicMono$" or type == "de.unikl.cs.dbis.waves.testjobs.split.LexicographicPartitionwise$":
        return run.get("done-cardinalities")[0] - run.get("start-GlobalOrder")[0]
    raise ValueError("unknown split type: " + type)

def parse_sort_order(run):
    type = run.get("split-start")[1]
    if type == "de.unikl.cs.dbis.waves.testjobs.split.ModelGini$":
        return 0
    if type == "de.unikl.cs.dbis.waves.testjobs.split.ModelGiniWithSort$":
        return run.get("end-GlobalOrder")[0] - run.get("start-GlobalOrder")[0] + run.get("end-PriorityStep")[0] - run.get("start-PriorityStep")[0]
    if type == "de.unikl.cs.dbis.waves.testjobs.split.LexicographicMono$":
        return run.get("end-DataframeSorter")[0] - run.get("done-cardinalities")[0]
    if type == "de.unikl.cs.dbis.waves.testjobs.split.LexicographicPartitionwise$":
        return run.get("end-GlobalOrder")[0] - run.get("done-cardinalities")[0] + run.get("end-ParallelSorter")[0] - run.get("start-ParallelSorter")[0]
    raise ValueError("unknown split type: " + type)

def parse_split(run):
    type = run.get("split-start")[1]
    if type == "de.unikl.cs.dbis.waves.testjobs.split.ModelGini$" or type == "de.unikl.cs.dbis.waves.testjobs.split.ModelGiniWithSort$":
        return run.get("end-ModelGini")[0] - run.get("start-ModelGini")[0] + run.get("end-Shuffle")[0] - run.get("start-ShuffleByShape")[0]
    if type == "de.unikl.cs.dbis.waves.testjobs.split.LexicographicMono$":
        return run.get("end-SingleBucket")[0] - run.get("start-SingleBucket")[0]
    if type == "de.unikl.cs.dbis.waves.testjobs.split.LexicographicPartitionwise$":
        return run.get("end-FlatShapeBuilder")[0] - run.get("start-ParallelEvenBuckets")[0]
    raise ValueError("unknown split type: " + type)

def parse_write(run):
    type = run.get("split-start")[1]
    if type == "de.unikl.cs.dbis.waves.testjobs.split.ModelGini$" or type == "de.unikl.cs.dbis.waves.testjobs.split.ModelGiniWithSort$":
        return run.get("end-PrioritySink")[0] - run.get("start-PrioritySink")[0]
    if type == "de.unikl.cs.dbis.waves.testjobs.split.LexicographicMono$":
        return run.get("end-DataframeSink")[0] - run.get("start-DataframeSink")[0]
    if type == "de.unikl.cs.dbis.waves.testjobs.split.LexicographicPartitionwise$":
        return run.get("end-ParallelSink")[0] - run.get("start-ParallelSink")[0]
    raise ValueError("unknown split type: " + type)

def parse_optional_metadata(run, name):
    if name in run:
        return run.get(name)[1]
    return ""

# Prepare Data
files = [read_run(logdir + '/' + filename) for filename in os.listdir(logdir) if filename.endswith(".csv")]
print("type,dataset,total_time,size_bytes,tree_location,bucket_count,setup_time,gather_data_time,split_time,sort_order_time,write_time,max_size,min_size,colum_splits,pruning,fingerprint_count,fingerprint_pruning,sampling,shuffle_partitions")
for run in files:
    type = run.get("split-start")[1]
    dataset = run.get("read-dataframe")[1]
    total_time = run.get("split-done")[0] - run.get("split-start")[0]
    size_bytes = run.get("metadata-bytesize")[1]
    tree_location = run.get("metadata-treeLocation")[1]
    bucket_count = run.get("metadata-bucketCount")[1]
    #bucket_count = 0
    setup_time = run.get("split-start")[0] - run.get("read-dataframe")[0]
    gather_data_time = parse_gather_data(run)
    #gather_data_time = 0
    split_time = parse_split(run)
    #split_time = 0
    sort_order_time = parse_sort_order(run)
    #sort_order_time = 0
    write_time = parse_write(run)
    #write_time = 0
    max_size = parse_optional_metadata(run, "parameter-maxSize")
    min_size = parse_optional_metadata(run, "parameter-minSize")
    colum_splits = parse_optional_metadata(run, "parameter-useColumnSplits")
    pruning = parse_optional_metadata(run, "parameter-useSearchSpacePruning")
    fingerprint_count = parse_optional_metadata(run, "metadata-fingerprintCount")
    fingerprint_pruning = parse_optional_metadata(run, "parameter-fingerprintPruning")
    sampling = parse_optional_metadata(run, "parameter-sampler")
    shuffle_partitions = parse_optional_metadata(run, "parameter-shufflePartitions")
    print(f"{type},{dataset},{total_time},{size_bytes},{tree_location},{bucket_count},{setup_time},{gather_data_time},{split_time},{sort_order_time},{write_time},{max_size},{min_size},{colum_splits},{pruning},{fingerprint_count},{fingerprint_pruning},{sampling},{shuffle_partitions}")
