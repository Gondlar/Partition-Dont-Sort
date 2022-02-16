package de.unikl.cs.dbis.waves

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.{util => ju}

/**
  * Most of this class reimplements FileDataSourceV2 because we need many
  * of its features but cannot provide a working Fallback Data Source
  * 
  * @see org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
  */
case class DefaultSource() extends TableProvider with DataSourceRegister {
    lazy val sparkSession = SparkSession.active

    override def shortName(): String = "waves"

    override def supportsExternalMetadata() = true
    override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform]
        = Array.empty

    protected def getPaths(map: CaseInsensitiveStringMap): Seq[String] = {
        val objectMapper = new ObjectMapper()
        val paths = Option(map.get("paths")).map(pathStr =>
            objectMapper.readValue(pathStr, classOf[Array[String]]).toSeq
        ).getOrElse(Seq.empty)
        paths ++ Option(map.get("path")).toSeq
    }

    private def getParameters(options : CaseInsensitiveStringMap) = {
        val paths = getPaths(options)
        val path = paths.size match {
            case 0 => throw QueryExecutionErrors.dataPathNotSpecifiedError()
            case 1 => paths(0)
            case _ => throw QueryExecutionErrors.multiplePathsSpecifiedError(paths)
        }
        (path, s"waves $path")
    }

    private var table : Option[WavesTable] = None

    override def inferSchema(options: CaseInsensitiveStringMap): StructType
        = table match {
            case Some(value) => value.schema()
            case None => {
                val (path, name) = getParameters(options)
                val tbl = WavesTable(name, sparkSession, path, options)
                val res = tbl.schema()
                table = Some(tbl)
                res
            }
        }

    override def getTable(schema: StructType, transform: Array[Transform], options: ju.Map[String,String]): Table
        = table match {
            case Some(value) => value
            case None => {
                val opt = new CaseInsensitiveStringMap(options)
                val (path, name) = getParameters(opt)
                WavesTable(name, sparkSession, path, opt, schema)
            }
        }
}
