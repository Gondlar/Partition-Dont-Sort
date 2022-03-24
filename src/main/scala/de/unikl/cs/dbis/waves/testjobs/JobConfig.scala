package de.unikl.cs.dbis.waves.testjobs

object JobConfig {
  val inputPath = "file:///cluster-share/benchmarks/json/twitter/109g_multiple"
  val hdfsHost = "hdfs://namenode:9000"
  val fallbackBlocksize = 128*1024*1024L
  val sampleSize = 10*1024*1024L

  val parquetPath = s"$hdfsHost/out/spill/"
  val parquetFormat = "parquet"

  val wavesPath = s"$hdfsHost/out/"
  val wavesFormat = "de.unikl.cs.dbis.waves"

  val completeScanColumn = "user.name"
  val completeScanValue = "xx"
  val partialScanColumn = "quoted_status.user.name"
  val partialScanValue = "xx"
}
