#!/usr/bin/env python3

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import os
import time

plot_width = .7
logdir = "/home/patrick/currentBatchLogs/queriesSwitch"

experiment_names = [ "UsernameStartsWith", "RetweeterUsernameStartsWith", "DeletedTweetsPerUser" ]

class Experiment:
    """
    Experiment wraps working with a run's dictionary representation and serves
    as a container for multiple runs of the same experiemnt.
    """

    __runs = []
    __identifier = ""
    __isWaves = False

    __QUERY_START = "query-start"
    __QUERY_RUN = "query-run"
    __QUERY_END = "query-end"
    __BUILD_SCAN = "build-scan"
    __SCAN_BUILT = "scan-built"
    __CHOSE_BUCKETS = "chose-buckets"

    def __init__(self, name, iswaves):
        """
        Initialize the experiment
        :param name: The class name of the experiment as specified in the log on query-start
        :param iswaves: Whether the experiment was run with waves (true) or Parquet (false) as specified in query-run
        """
        self.__runs = []
        self.__identifier = f"de.unikl.cs.dbis.waves.testjobs.query.{name}$"
        self.__isWaves = iswaves
    
    def typeString(self):
        """
        A string representation for type of this Experiemnt. Either "Parquet" or "Waves"
        """
        if self.__isWaves:
            return "Waves"
        else:
            return "Parquet"

    def __str__(self):
        return f"{self.__identifier}-{self.typeString()}: {self.__runs}"

    def __repr__(self):
        return self.__str__()

    def getName(self):
        return self.__identifier

    def isWaves(self):
        return self.__isWaves

    def addIfMember(self, run: dict):
        """
        Append a log file to this experiment if it is a run of this experiment
        :param run: the log file as a dict
        """
        if run.get(self.__QUERY_START)[1] == self.getName() and run.get(self.__QUERY_RUN)[1].lower() == self.__isWaves.__str__().lower():
            self.__runs.append(run)

    def getUniqueResult(self):
        """
        Normally, all runs of an experiment should produce the same result.
        This method retrieves this result or terminates if the experiments
        have differing results.
        :return: this experiements result
        """
        values = np.array([run[self.__QUERY_END][1] for run in self.__runs])
        if np.all(values == values[0]):
            return values[0]
        else:
            print("Results are not unique for experiment " + self.__identifier)
            print(self.__runs)
            exit(1)


    def haveEqualResults(self, other):
        """
        Check whether the given Experiment has the same result as this one.
        Should this not be the case, we terminate.
        :param other: the other experiement
        """
        myResult = self.getUniqueResult()
        otherResult = other.getUniqueResult()
        if myResult != otherResult:
            print("Results differ between " + self.__identifier + " and " + other.__identifier)
            print(myResult + " vs. " + otherResult)
            exit(1)
    
    def matches(self, other):
        """
        Check whether the given Experiment is this one's partner, i.e., has the
        same name but the opposite type
        """
        return self.getName() == other.getName() and self.__isWaves != other.__isWaves

    def getTimes(self):
        """
        Get the times for all runs of this experiment
        :return: A list of tuples of runtimes. Each tuple contains the time to create a data frame, the time to choose buckets, the time to build a scan and the remaining runtime for each run.
        """
        times = []
        for run in self.__runs:
            total_time = run[self.__QUERY_END][0] - run[self.__QUERY_START][0]
            run_time = run[self.__QUERY_END][0] - run[self.__QUERY_RUN][0]
            spark_time = total_time - run_time
            if self.__isWaves:
                chose_buckets = run[self.__CHOSE_BUCKETS][0] - run[self.__BUILD_SCAN][0]
                scan_built = run[self.__SCAN_BUILT][0] - run[self.__CHOSE_BUCKETS][0]
                run_time -= chose_buckets + scan_built
                times.append((spark_time, chose_buckets, scan_built, run_time))
            else:
                times.append((spark_time, 0, 0, run_time))
        return times

def quotes(s: str):
    """
    Remove quote marks from a string if they are present
    :param s: the string to strip
    :return: the stripped string
    """
    if s[0] == '\'' and s[-1] == '\'':
        return s[1:-1]
    return s

def crop(s: str, n: int):
    """
    Crop a String to at most n characters
    :param s: the string
    :param n: the number of characters
    :return: the cropped string
    """
    if len(s) > n:
        return f"{s[0:n-3]}..."
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

# Prepare Data
experiments = [ Experiment(name, type) for name in experiment_names for type in [True, False]]
files = [logdir + '/' + filename for filename in os.listdir(logdir) if filename.endswith(".csv")]
for filename in files:
    run = read_run(filename)
    for exp in experiments:
        exp.addIfMember(run)
for exp1 in experiments:
    for exp2 in experiments:
        if exp1.matches(exp2):
            exp1.haveEqualResults(exp2)
results = np.array([np.median(exp.getTimes(), axis=0) for exp in experiments])
results = np.transpose(results)
ind = np.arange(len(experiments))

# Speedup
total = np.sum(results, axis=0)
totalWithoutInit = total - results[0]
for i, exp in enumerate(experiments):
    if exp.isWaves():
        continue
    for j, partner in enumerate(experiments):
        if partner.matches(exp):            
            print(f"{exp.getName()} Speedup: {total[i]/total[j]} / {totalWithoutInit[i]/totalWithoutInit[j]}")
            break
print(total)
print(totalWithoutInit)

# Plot
plt.figure(num=None, figsize=(4,3), dpi=150)
plt.bar(ind, results[0], plot_width, label='Initialize Spark', capsize=3, color="tab:blue") # yerr=results[4], 
plt.bar(ind, results[1], plot_width, label='Choose Buckets', bottom=results[0], capsize=3, color="tab:orange")
plt.bar(ind, results[2], plot_width, label='Build Scan', bottom=results[0]+results[1], capsize=3, color="gold")
plt.bar(ind, results[3], plot_width, label='Run Query', bottom=results[0]+results[1]+results[2], capsize=3, color="tab:green")
plt.xticks(ind, [f"{crop(exp.getName()[38:-1], 15)}\n{exp.typeString()}" for exp in experiments])
#plt.xticks(ind, ["Q1\nNormal", "Q1\nPartitioned", "Q2\nNormal", "Q2\nPartitioned"])

ax = plt.gca()
formatter = matplotlib.ticker.FuncFormatter(lambda ms, x: time.strftime('%S', time.gmtime(ms // 1000)))
ax.yaxis.set_major_formatter(formatter)
plt.ylabel('Runtime in Seconds')

ax.legend(loc="upper right", bbox_to_anchor=(1.57, 1.03), ncol=1)

plt.savefig("evaluate.png", format="png", bbox_inches='tight')
plt.savefig("evaluate.pdf", format="pdf", bbox_inches='tight')
plt.show()
