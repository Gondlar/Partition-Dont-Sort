#!/bin/bash

for N in 1 2 .. 10
do
    sbt "runMain de.unikl.cs.dbis.waves.testjobs.CompleteScanNormal" --batch
    sbt "runMain de.unikl.cs.dbis.waves.testjobs.CompleteScanWaves" --batch
    sbt "runMain de.unikl.cs.dbis.waves.testjobs.PartialScanNormal" --batch
    sbt "runMain de.unikl.cs.dbis.waves.testjobs.PartialScanWaves" --batch
done
