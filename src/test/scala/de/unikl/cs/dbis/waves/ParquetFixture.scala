package de.unikl.cs.dbis.waves

import org.scalatest.Suite

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.spark.sql.SparkSession

trait ParquetFixture { this: Suite =>

  @annotation.nowarn("msg=constructor ParquetFileReader in class ParquetFileReader is deprecated")
  def readParquetSchema(spark: SparkSession, path: Path)
    = new ParquetFileReader(
        spark.sparkContext.hadoopConfiguration,
        path,
        ParquetMetadataConverter.NO_FILTER
      ).getFileMetaData().getSchema()
}
