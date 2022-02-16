package de.unikl.cs.dbis.waves

import org.apache.hadoop.fs.{Path,FileSystem}

import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.connector.catalog.{Table,TableCapability,SupportsRead,SupportsWrite}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo,WriteBuilder,SupportsTruncate}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

import de.unikl.cs.dbis.waves.partitions.{PartitionTree,PartitionByInnerNode}

import java.{util => ju}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer

class WavesTable private (
    override val name : String,
    spark : SparkSession,
    basePath : String,
    val fs : FileSystem,
    options : CaseInsensitiveStringMap,
    private var partitionTree : PartitionTree
) extends Table with SupportsRead with SupportsWrite {

    override def capabilities(): ju.Set[TableCapability]
        = Set( TableCapability.BATCH_READ
             , TableCapability.BATCH_WRITE
             , TableCapability.TRUNCATE
             ).asJava //TODO Truncate

    override def schema(): StructType = partitionTree.globalSchema

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder
        = new WavesScanBuilder(this, options)
    
    override def newWriteBuilder(options: LogicalWriteInfo): WriteBuilder
        = WavesWriteBuilder(this, options)

    private[waves] def makeDelegateTable(partitions : PartitionFolder*) : ParquetTable = {
        val paths = partitions.map(_.filename)
        ParquetTable( s"Delegate ParquetTable for ${name}"
                    , spark
                    , options
                    , paths
                    , Some(schema())
                    , classOf[ParquetFileFormat])
    }

    private[waves] def findRequiredPartitions(filters : Iterable[Filter]) : Seq[PartitionFolder]
        = partitionTree.getBuckets(filters)
                       .map(bucket => bucket.folder(basePath))
                       .toSeq
    
    private[waves] def createSpillPartition()
        = partitionTree.createSpillPartition(() => PartitionFolder.makeFolder(basePath, fs, false).name)
    
    private[waves] def writePartitionScheme() = {
        val json = partitionTree.toJson.getBytes(StandardCharsets.UTF_8)
        val out = fs.create(WavesTable.makeSchemaPath(basePath))
        out.write(json)
        out.close()
    }

    private[waves] def insertLocation() = (partitionTree.fastInsertLocation match {
            case Some(value) => value
            case None => createSpillPartition()
        }).folder(basePath)
    
    private[waves] def truncate() = {
        partitionTree = new PartitionTree(schema)
        insertLocation()
    }

    def repartition(partitionName: String, key: String) = {
        // Find partition
        //This is inefficient for very large trees
        val partition = partitionTree.getBuckets().find(bucket => bucket.name == partitionName).get

        // Modify partition tree
        val partitionWithKey = PartitionFolder.makeFolder(basePath, fs)
        val partitionWithoutKey = PartitionFolder.makeFolder(basePath, fs)
        val newNode = PartitionByInnerNode(key, partitionWithKey.name, partitionWithoutKey.name)
        partitionTree.replace(partition, newNode)

        // Repartition
        val df = spark.read.format("parquet").load(partition.folder(basePath).filename)
        //This is inefficient because it neds two scans
        val dfWithoutKey = df.filter(col(key).isNull)
        val dfWithKey = df.filter(col(key).isNotNull)
        var success = false
        try {
            dfWithoutKey.write.mode(SaveMode.Overwrite).format("parquet").save(partitionWithoutKey.filename)
            dfWithKey.write.mode(SaveMode.Overwrite).format("parquet").save(partitionWithKey.filename)
            success = true
        } finally {
            if (success) {
                partitionWithKey.moveFromTempToFinal(fs)
                partitionWithoutKey.moveFromTempToFinal(fs)
                //TODO delete mechanism for old folders
                writePartitionScheme()
            } else {
                partitionWithKey.delete(fs)
                partitionWithoutKey.delete(fs) 
            }
        }
    }
}

object WavesTable {
    def makeSchemaPath(basePath : String) = new Path(s"$basePath/schema.json")

    def apply(name : String, spark : SparkSession, basePath : String, options : CaseInsensitiveStringMap) = {
        val schemaPath = makeSchemaPath(basePath)
        val fs = schemaPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val partitionTree = if (fs.exists(schemaPath)) {
            PartitionTree.fromJson(readSchema(schemaPath, fs))
        } else {
            throw QueryCompilationErrors.dataSchemaNotSpecifiedError("waves")
        }
        new WavesTable(name, spark, basePath, fs, options, partitionTree)
    }

    def apply(name : String, spark : SparkSession, basePath : String, options : CaseInsensitiveStringMap, schema : StructType) = {
        val partitionTree = new PartitionTree(schema)
        val fs = new Path(basePath).getFileSystem(spark.sparkContext.hadoopConfiguration)
        new WavesTable(name, spark, basePath, fs, options, partitionTree)
    }

    private def readSchema(schemaPath: Path, fs: FileSystem) = {
        val res = ArrayBuffer.empty[Byte]
        val buffer = new Array[Byte](256*256) // Possibly use differing size
        val reader = fs.open(schemaPath)
        var read = 0
        while ({read = reader.read(buffer); read != -1}) {
        res.addAll(buffer.slice(0, read))
        }
        new String(res.toArray, StandardCharsets.UTF_8)
    }
}
