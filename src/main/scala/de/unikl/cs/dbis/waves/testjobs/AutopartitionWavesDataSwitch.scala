package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import de.unikl.cs.dbis.waves.util.Logger
import de.unikl.cs.dbis.waves.WavesTable
import de.unikl.cs.dbis.waves.split.RecursiveSplitter
import de.unikl.cs.dbis.waves.split.recursive.switchHeuristic

object AutopartitionWavesDataSwitch {
    def main(args: Array[String]) : Unit = {
        Logger.log("job-start")
        val appName = "Autopartition WavesData Switch"
        val conf = new SparkConf().setAppName(appName)
        //conf.setMaster("local") // comment this line to run on the cluster
        val spark = SparkSession.builder().config(conf).getOrCreate()

        Logger.log("initialize-start")
        val df = spark.read.format("json").load(JobConfig.inputPath)
        df.write.mode(SaveMode.Overwrite).format(JobConfig.wavesFormat).save(JobConfig.wavesPath)
        val relation = WavesTable(s"Repartition ${JobConfig.wavesPath}", spark, JobConfig.wavesPath, CaseInsensitiveStringMap.empty())
        Logger.log("convert-done", relation.diskSize())
        RecursiveSplitter( relation
                         , spark.sparkContext.hadoopConfiguration.getLong("dfs.blocksize", JobConfig.fallbackBlocksize)
                         , JobConfig.sampleSize
                         , switchHeuristic _)
            .partition()
        Logger.log("partition-done", relation.diskSize())
        relation.defrag()
        relation.vacuum()
        Logger.log("initialize-end", relation.diskSize())

        Logger.log("job-end")
        Logger.flush(spark.sparkContext.hadoopConfiguration)
        spark.stop()
    }
}