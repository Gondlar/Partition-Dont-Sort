package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import de.unikl.cs.dbis.waves.partitions.Absent
import de.unikl.cs.dbis.waves.util.Logger
import de.unikl.cs.dbis.waves.WavesTable

object InitializeWavesData {
    def main(args: Array[String]) : Unit = {
        Logger.log("job-start")
        val appName = "InitializeWavesData"
        val conf = new SparkConf().setAppName(appName)
        //conf.setMaster("local") // comment this line to run on the cluster
        val spark = SparkSession.builder().config(conf).getOrCreate()

        Logger.log("initialize-start")
        val df = spark.read.format("json").load(JobConfig.inputPath)
        df.write.mode(SaveMode.Overwrite).format(JobConfig.wavesFormat).save(JobConfig.wavesPath)
        val relation = WavesTable(s"Repartition ${JobConfig.wavesPath}", spark, JobConfig.wavesPath, CaseInsensitiveStringMap.empty())
        Logger.log("convert-done", relation.diskSize())
        relation.repartition("quoted_status")
        Logger.log("repartition-1", relation.diskSize())
        relation.repartition("retweeted_status", Absent)
        Logger.log("repartition-2", relation.diskSize())
        relation.repartition("delete", Absent, Absent)
        Logger.log("repartition-3", relation.diskSize())
        relation.vacuum()
        Logger.log("initialize-end")

        Logger.log("job-end")
        Logger.flush(spark.sparkContext.hadoopConfiguration)
        spark.stop()
    }
}