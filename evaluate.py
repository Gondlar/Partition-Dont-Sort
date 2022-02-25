#!/usr/bin/env python3

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import os
import time

plot_width = .7
logdir = "/home/patrick/log"

class Experiment:
    __runs = []
    __identifier = ""
    __isWaves = False

    __JOB_START = "job-start"
    __JOB_END = "job-end"
    __BUILD_SCAN = "build-scan"
    __SCAN_BUILT = "scan-built"
    __CHOSE_BUCKETS = "chose-buckets"

    def _run_start(self):
        return self.__identifier + "-start"

    def _run_end(self):
        return self.__identifier + "-end"

    def __init__(self, name, iswaves):
        self.__runs = []
        self.__identifier = name
        self.__isWaves = iswaves

    def __str__(self):
        return self.__identifier + ": " + self.__runs.__str__()

    def __repr__(self):
        return self.__str__()

    def getName(self):
        return self.__identifier

    def addIfMember(self, run):
        if self._run_start() in run:
            self.__runs.append(run)

    def getUniqueResult(self):
        values = np.array([run[self._run_end()][1] for run in self.__runs])
        if np.all(values == values[0]):
            return values[0]
        else:
            print("Results are not unique for experiment " + self.__identifier)
            print(self.__runs)
            exit(1)

    def haveEqualResults(self, other):
        myResult = self.getUniqueResult()
        otherResult = other.getUniqueResult()
        if myResult != otherResult:
            print("Results differ between " + self.__identifier + " and " + other.__identifier)
            print(myResult + " vs. " + otherResult)
            exit(1)
    
    def getTimes(self):
        times = []
        for run in self.__runs:
            total_time = run[self.__JOB_END][0] - run[self.__JOB_START][0]
            run_time = run[self._run_end()][0] - run[self._run_start()][0]
            spark_time = total_time - run_time
            if self.__isWaves:
                chose_buckets = run[self.__CHOSE_BUCKETS][0] - run[self.__BUILD_SCAN][0]
                scan_built = run[self.__SCAN_BUILT][0] - run[self.__CHOSE_BUCKETS][0]
                run_time -= chose_buckets + scan_built
                times.append((spark_time, chose_buckets, scan_built, run_time))
            else:
                times.append((spark_time, 0, 0, run_time))
        return times

experiments = [ Experiment("completeParquet", False)
              , Experiment("completeWaves", True)
              , Experiment("partialParquet", False)
              , Experiment("partialWaves", True)
              ]

def quotes(s):
    if s[0] == '\'' and s[-1] == '\'':
        return s[1:-1]
    return s

def read_run(path):
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

# Prepare Data
files = [logdir + '/' + filename for filename in os.listdir(logdir) if filename.endswith(".csv")]
for filename in files:
    run = read_run(filename)
    for exp in experiments:
        exp.addIfMember(run)
experiments[0].haveEqualResults(experiments[1])
experiments[2].haveEqualResults(experiments[3])
results = np.array([np.median(exp.getTimes(), axis=0) for exp in experiments])
results = np.transpose(results)
ind = np.arange(len(experiments))

# Speedup
total = np.sum(results, axis=0)
totalWithoutInit = total - results[0]
print("Complete Scan Speedup: " + str((total[0]/total[1])) + " / " + str((totalWithoutInit[0]/totalWithoutInit[1])))
print("Partial Scan Speedup: " + str((total[2]/total[3])) + " / " + str((totalWithoutInit[2]/totalWithoutInit[3])))

# Plot
plt.figure(num=None, figsize=(4,3), dpi=150)
plt.bar(ind, results[0], plot_width, label='Initialize Spark', capsize=3, color="tab:blue") # yerr=results[4], 
plt.bar(ind, results[1], plot_width, label='Choose Buckets', bottom=results[0], capsize=3, color="tab:orange")
plt.bar(ind, results[2], plot_width, label='Build Scan', bottom=results[0]+results[1], capsize=3, color="gold")
plt.bar(ind, results[3], plot_width, label='Run Query', bottom=results[0]+results[1]+results[2], capsize=3, color="tab:green")
#plt.xticks(ind, [exp.getName() for exp in experiments])
plt.xticks(ind, ["Complete\nParquet", "Complete\nPartitioned", "Partial\nParquet", "Partial\nPartitioned"])

ax = plt.gca()
formatter = matplotlib.ticker.FuncFormatter(lambda ms, x: time.strftime('%M:%S', time.gmtime(ms // 1000)))
ax.yaxis.set_major_formatter(formatter)
plt.ylabel('Runtime in Seconds')

ax.legend(loc="upper right", bbox_to_anchor=(1.57, 1.03), ncol=1)

plt.savefig("evaluate.png", format="png", bbox_inches='tight')
plt.show()