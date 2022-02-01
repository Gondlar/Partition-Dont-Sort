package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.{SparkConf,SparkContext}
import de.unikl.cs.dbis.waves.util.Logger

object SystemSetupTestJob {
  def main(args: Array[String]) : Unit = {
      Logger.log("job-start")
      val appName = "SystemSetupTestJob"
      val conf = new SparkConf().setAppName(appName)
      conf.setMaster("local") // comment this line to run on the cluster
      val sc = new SparkContext(conf)

      Logger.log("test-start")
      val data = Seq(1, 2, 3, 4, 5)
      val distData = sc.parallelize(data)
      val double = distData.map(x => x*2)
      val sum = double.reduce(_+_)
      Logger.log("test-end", sum)

      Logger.log("job-end")
      Logger.flush(sc.hadoopConfiguration)
      sc.stop()
  }
}
