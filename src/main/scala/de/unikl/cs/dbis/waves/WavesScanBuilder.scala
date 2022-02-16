package de.unikl.cs.dbis.waves

import org.apache.spark.sql.connector.read.{
    ScanBuilder,
    Scan,
    SupportsPushDownRequiredColumns,
    SupportsPushDownFilters
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.sources.Filter

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import de.unikl.cs.dbis.waves.util.Logger

class WavesScanBuilder(
    private val table : WavesTable,
    private val options : CaseInsensitiveStringMap
) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

    
    private var filters = Array.empty[Filter]
    override def pushFilters(filters: Array[Filter]): Array[Filter] = {
        this.filters = filters
        this.filters
    }

    override def pushedFilters(): Array[Filter] = filters

    var prunedSchema : Option[StructType] = None
    override def pruneColumns(schema: StructType): Unit
        = prunedSchema = Some(schema)
    
    override def build(): Scan = {
        Logger.log("build-scan")
        // Scan partitions
        val partitions = table.findRequiredPartitions(filters)
        Logger.log("chose-buckets", partitions.mkString(";"))

        // Make delegate
        val parquetTable = table.makeDelegateTable(partitions:_*)
        val delegate = parquetTable.newScanBuilder(options)

        // Pushdown to delegate
        if (!filters.isEmpty) delegate.pushFilters(filters)
        prunedSchema match {
            case Some(value) => delegate.pruneColumns(value)
            case None => ()
        }

        // Build the scan using the delefate
        val res = delegate.build()
        Logger.log("scan-built")
        res
    }
}
