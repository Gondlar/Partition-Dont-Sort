package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import de.unikl.cs.dbis.waves.pipeline.sample._

/**
  * Configuration for our jobs 
  *
  * @param options a map of options
  */
class JobConfig(options: Map[String, String] = Map.empty) {
  private val myOptions = options.map({ case (key, value) => (key.toLowerCase, value)})

  /**
    * Check if the given key is set in this configuration. If it isn't , it will
    * yield the default value (if any).
    *
    * @param key the key
    * @return true iff it is explicitly set
    */
  def isSet(key: String) = myOptions.contains(key.toLowerCase())

  /**
    * fetch the value for the option with the given name as a String
    *
    * @param key case-insensitive option name
    * @return the value or None if it is unset
    */
  def getString(key: String) = myOptions.get(key.toLowerCase)

  /**
    * fetch the value for the option with the given name as an Int
    *
    * @param key case-insensitive option name
    * @return the value or None if it is unset or not a Int
    */
  def getInt(key: String) = getString(key).flatMap(v =>
    try Some(v.toInt) catch {
      case e : NumberFormatException => None
    }
  )

  /**
    * fetch the value for the option with the given name as a Long
    *
    * @param key case-insensitive option name
    * @return the value or None if it is unset or not a Long
    */
  def getLong(key: String) = getString(key).flatMap(v =>
    try Some(v.toLong) catch {
      case e : NumberFormatException => None
    }
  )

  /**
    * fetch the value for the option with the given name as a Double
    *
    * @param key case-insensitive option name
    * @return the value or None if it is unset or not a Double
    */
  def getDouble(key: String) = getString(key).flatMap(v =>
    try Some(v.toDouble) catch {
      case e : NumberFormatException => None
    }
  )

  /**
    * fetch the value for the option with the given name as a Boolean
    *
    * @param key case-insensitive option name
    * @return the value or None if it is unset or not a Boolean
    */
  def getBool(key: String) = getString(key).flatMap(v =>
    try Some(v.toBoolean) catch {
      case e : IllegalArgumentException => None
    }
  )

  /**
    * @return true iff the master is set to local
    */
  def isLocal = master == Some("local")

  // All of the following options simply fetch an appropriately typed value with
  // the same key as the method name. Most of them supply a default value if the
  // value is unset

  def master = getString("master")

  def inputPath = getString("inputPath").getOrElse("file:///cluster-share/benchmarks/json/twitter/109g_multiple")
  def inputSchemaPath = getString("inputSchemaPath")
  def filesystem = getString("filesystem").getOrElse(
      if (isLocal) "file://" + System.getProperty("user.dir") else "hdfs://namenode:9000"
  )
  def fallbackBlocksize = getLong("fallbackBlocksize").getOrElse(128*1024*1024L)
  def sampleSize = getLong("sampleSize").getOrElse(10*1024*1024L)
  def numPartitions = getInt("numPartitions")
  def relativeMinSize = getDouble("relativeMinSize").getOrElse(.5)
  def useColumnSplits = getBool("useColumnSplits").getOrElse(false)
  def useSearchSpacePruning = getBool("useSearchSpacePruning").getOrElse(false)
  def useFingerprintPruning = getDouble("useFingerprintPruning")
  def useExactCardinalities = getBool("useExactCardinalities").getOrElse(false)
  def useSampler = getString("sampler").map(str => {str match {
      case "uniform" if isSet("uniformRate") => UniformSampler(getDouble("uniformRate").get)
      case "init" if isSet("perPartition") => InitSample(getInt("perPartition").get)
      case _ => {
        System.err.println(s"WARN: Ignoring unknown sampler option '$str'")
        NullSampler
      }
    }
  }).getOrElse(NullSampler)

  def wavesPath = getString("wavesPath").getOrElse(s"$filesystem/out/")
  def treeStorageDirectory = getString("treeStorageDirectory").getOrElse(s"./trees")
  def useWaves = getBool("useWaves").getOrElse(true)
  def cleanWavesPath = getBool("cleanWavesPath").getOrElse(false)
  def modifySchema = getBool("modifySchema").getOrElse(false)

  def completeScanColumn = getString("completeScanColumn").getOrElse("user.name")
  def partialScanColumn = getString("partialScanColumn").getOrElse("quoted_status.user.name")
  def scanValue = getString("scanValue").getOrElse("xx")

  /**
    * Construct a Spark session using the configured data
    *
    * @param name the application name
    * @return the spark session
    */
  def makeSparkSession(name: String) = {
    val conf = new SparkConf().setAppName(name)
    master match {
      case Some(value) => conf.setMaster(value)
      case None => 
    }
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}

object JobConfig {

  /**
    * parse the supplied arguments to construct a JobConfig. Arguments must have
    * the form "key=value"
    *
    * @param args the arguemnts to parse
    * @return the constructed JobConfig
    */
  def fromArgs(args: Array[String]) = {
    val builder = Map.newBuilder[String, String]
    for (arr <- args.iterator.map(_.split("=")).filter(_.length == 2)) {
      builder += ((arr(0), arr(1)))
    }
    val map = builder.result()
    if (map.size != args.size)
        throw new IllegalArgumentException(s"At least one Argument could not be parsed: $args")
    new JobConfig(map)
  }
}
