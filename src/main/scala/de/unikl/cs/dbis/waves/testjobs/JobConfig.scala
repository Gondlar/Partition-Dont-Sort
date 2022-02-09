package de.unikl.cs.dbis.waves.testjobs

object JobConfig {
  val parquetPath = "out/spill/"
  val parquetFormat = "parquet"

  val wavesPath = "out/"
  val wavesFormat = "de.unikl.cs.dbis.waves"

  val completeScanColumn = "user.name"
  val completeScanValue = "xx"
  val partialScanColumn = "quoted_status.user.name"
  val partialScanValue = "xx"
}
