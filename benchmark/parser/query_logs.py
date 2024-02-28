#!/usr/bin/env python3

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import os
import time

#logdir = "/home/patrick/Promotion/Publications/partition-dont-sort/results/queries/ModelGini-250-90"
logdir = "/home/patrick/query_logs/ModelGini-250-0.5"

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

def parse_optional_metadata(run, name):
    if name in run:
        return run.get(name)[1]
    return ""

# Prepare Data
all_files = [read_run(logdir + '/' + filename) for filename in os.listdir(logdir) if filename.endswith(".csv")]
files = [x for x in all_files if "query-start" in x]

query_count = 0
while ("query-run-" + str(query_count)) in files[0]:
    query_count += 1

print("use_skipping,total,init," + ",".join(["query" + str(x) for x in range(query_count)]))
for run in files:
    use_skipping = parse_optional_metadata(run, "parameter-use-waves")
    total = run.get("query-run-" + str(query_count-1))[0] - run.get("query-start")[0] 
    init = run.get("query-run-0")[0] - run.get("query-start")[0]
    queries = [str(run.get("query-end-" + str(x))[0] - run.get("query-run-" + str(x))[0]) for x in range(query_count)]
    print(f"{use_skipping},{total},{init}," + ",".join(queries))
