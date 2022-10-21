package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import de.unikl.cs.dbis.waves.util.Logger
import de.unikl.cs.dbis.waves.WavesTable
import de.unikl.cs.dbis.waves.split.RecursiveSplitter
import de.unikl.cs.dbis.waves.split.recursive.SwitchHeuristic

import WavesTable._

object AutopartitionWavesDataSwitch {
    def main(args: Array[String]) : Unit = {
        val jobConfig = JobConfig.fromArgs(args)

        Logger.log("job-start")
        val spark = jobConfig.makeSparkSession("Autopartition WavesData Switch")

        Logger.log("initialize-start")
        val df = spark.read.format("json").load(jobConfig.inputPath)
        df.write.mode(SaveMode.Overwrite).waves(jobConfig.wavesPath, df.schema)
        val relation = WavesTable(s"Repartition ${jobConfig.wavesPath}", spark, jobConfig.wavesPath, CaseInsensitiveStringMap.empty())
        Logger.log("convert-done", relation.diskSize())
        RecursiveSplitter( spark.sparkContext.hadoopConfiguration.getLong("dfs.blocksize", jobConfig.fallbackBlocksize)
                         , jobConfig.sampleSize
                         , SwitchHeuristic)
            .prepare(relation)
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