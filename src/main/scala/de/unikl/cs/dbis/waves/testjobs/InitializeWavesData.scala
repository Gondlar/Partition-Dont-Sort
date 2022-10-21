package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import de.unikl.cs.dbis.waves.partitions.{SplitByPresence,Bucket}
import de.unikl.cs.dbis.waves.split.PredefinedSplitter
import de.unikl.cs.dbis.waves.util.Logger
import de.unikl.cs.dbis.waves.WavesTable

import WavesTable._

object InitializeWavesData {
    def main(args: Array[String]) : Unit = {
        val jobConfig = JobConfig.fromArgs(args)
        println(jobConfig.isLocal)

        Logger.log("job-start")
        val spark = jobConfig.makeSparkSession("InitializeWavesData")

        Logger.log("initialize-start")
        val df = spark.read.json(jobConfig.inputPath)
        df.write.mode(SaveMode.Overwrite).waves(jobConfig.wavesPath, df.schema)
        val manualShape = SplitByPresence( "quoted_status"
                                         , Bucket("quotes")
                                         , SplitByPresence( "retweeted_status"
                                                          , Bucket("retweets")
                                                          , SplitByPresence( "delete"
                                                                           , "deletes"
                                                                           , "normal"
                                                                           )
                                                          )
                                         )
        df.saveAsWaves(new PredefinedSplitter(manualShape), jobConfig.wavesPath)
        val relation = spark.read.waves(jobConfig.wavesPath).getWavesTable.get
        Logger.log("partition-done", relation.diskSize())
        relation.vacuum()
        Logger.log("initialize-end")

        Logger.log("job-end")
        Logger.flush(spark.sparkContext.hadoopConfiguration)
        spark.stop()
    }
}