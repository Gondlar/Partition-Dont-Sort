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
import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import de.unikl.cs.dbis.waves.partitions.Bucket

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

    def repartition(key: String, path : String *) = {
        // Find partition
        val partition = partitionTree.find(path) match {
            case Some(bucket@Bucket(_)) => bucket
            case _ => throw new RuntimeException("Partition not found")
        }

        // Modify partition tree
        val partitionWithKey = PartitionFolder.makeFolder(basePath, fs, false)
        val partitionWithoutKey = PartitionFolder.makeFolder(basePath, fs, false)
        val newNode = PartitionByInnerNode(key, partitionWithKey.name, partitionWithoutKey.name)
        partitionTree.replace(partition, newNode)

        // Repartition
        val repartitionHelperColumn = "__WavesRepartitionCol__"
        val tempFolder = PartitionFolder.makeFolder(basePath, fs)
        val df = spark.read.format("parquet").load(partition.folder(basePath).filename)
        try {
            df.withColumn(repartitionHelperColumn, col(key).isNull)
              .write.format("parquet")
                    .mode(SaveMode.Overwrite)
                    .partitionBy(repartitionHelperColumn)
                    .save(tempFolder.filename)
            fs.rename(new Path(s"${tempFolder.filename}/$repartitionHelperColumn=true"), partitionWithoutKey.file)
            fs.rename(new Path(s"${tempFolder.filename}/$repartitionHelperColumn=false"), partitionWithKey.file)
        } catch {
            case e : Throwable => {
                partitionWithKey.delete(fs)
                partitionWithoutKey.delete(fs) 
                throw e
            }
        } finally {
            tempFolder.delete(fs)
        }
        //TODO delete mechanism for old folders
        writePartitionScheme()
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
            res ++= buffer.slice(0, read)
        }
        new String(res.toArray, StandardCharsets.UTF_8)
    }
}
