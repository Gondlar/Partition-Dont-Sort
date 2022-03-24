package de.unikl.cs.dbis.waves

import org.apache.hadoop.fs.{Path,FileSystem}

import org.apache.spark.sql.{SparkSession,SaveMode,Row}
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

import de.unikl.cs.dbis.waves.partitions.{PartitionTree,PartitionByInnerNode,Bucket}
import de.unikl.cs.dbis.waves.util.{PathKey,SchemaMetric,Logger}

import java.{util => ju}
import java.nio.charset.StandardCharsets
import collection.JavaConverters._
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
        = partitionTree.createSpillPartition(() => PartitionFolder.makeFolder(basePath, false).name)
    
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

    def diskSize() : Long = {
        val foo = partitionTree.getBuckets()
        foo.map(_.folder(basePath).diskSize(fs)).sum
    }

    def repartition(key: String, path : String *) : Unit = {
        // Find partition
        val partition = partitionTree.find(path) match {
            case Some(bucket@Bucket(_)) => bucket
            case _ => throw new RuntimeException("Partition not found")
        }
        repartition(key, partition)
    }

    private def repartition(key: String, partition : Bucket) : (PartitionFolder, PartitionFolder) = {
        // Modify partition tree
        val partitionWithKey = PartitionFolder.makeFolder(basePath, false)
        val partitionWithoutKey = PartitionFolder.makeFolder(basePath, false)
        val newNode = PartitionByInnerNode(key, partitionWithKey.name, partitionWithoutKey.name)
        partitionTree.replace(partition, newNode)

        // Repartition
        val repartitionHelperColumn = "__WavesRepartitionCol__"
        val tempFolder = PartitionFolder.makeFolder(basePath)
        val df = spark.read.format("parquet").load(partition.folder(basePath).filename)
        try {
            df.withColumn(repartitionHelperColumn, col(key).isNull)
              .write.format("parquet")
                    .mode(SaveMode.Overwrite)
                    .partitionBy(repartitionHelperColumn)
                    .save(tempFolder.filename)
            val absent = new PartitionFolder(tempFolder.filename, s"$repartitionHelperColumn=true", false)
            partitionWithoutKey.moveFrom(absent, fs)
            val present = new PartitionFolder(tempFolder.filename, s"$repartitionHelperColumn=false", false)
            partitionWithKey.moveFrom(present, fs)
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
        (partitionWithKey, partitionWithoutKey)
    }

    def partition(threshold : Long, sampleSize : Long, metric: (Array[Row], Seq[PathKey], Seq[PathKey], Int) => Option[PathKey]) : Unit = {
        assert(partitionTree.root.isInstanceOf[Bucket])

        partition(threshold, sampleSize, metric, Seq.empty, Seq.empty, Seq.empty)
    }

    private def partition(threshold : Long, sampleSize : Long, metric: (Array[Row], Seq[PathKey], Seq[PathKey], Int) => Option[PathKey], knownAbsent : Seq[PathKey], knownPresent: Seq[PathKey], path: Seq[String]) : Unit = {
        // Get Current Partition data
        val currentPartition = partitionTree.find(path).get.asInstanceOf[Bucket]
        val currentFolder = currentPartition.folder(basePath)
        val diskSize = currentFolder.diskSize(fs)
        val sampleRate = sampleSize.toDouble/diskSize
        val cutoff = (threshold*0.9)/diskSize

        // read data, calculate metric and repartition
        val data = {
            val tmp = spark.read.format("parquet").schema(partitionTree.globalSchema).load(currentFolder.filename)
            if (sampleRate < 1) tmp.sample(sampleRate) else tmp
        }.collect()
        println(s"$path -> ${currentFolder.name}: ${diskSize.toDouble/threshold} with ${data.size} entries")
        metric(data, knownAbsent, knownPresent, (cutoff*data.size).toInt) match {
            case None => Logger.log("partition-abort", "metric shows no improvement")
            case Some(best) => {
                Logger.log("partition-by", best.toString)
                var (presentFolder, absentFolder) = repartition(best.toString, currentPartition)

                // recurse if data is larger than threshold
                val presentSize = presentFolder.diskSize(fs)
                Logger.log("partiton-present", presentSize/threshold.toDouble)
                if (presentSize > threshold)
                    partition(threshold, sampleSize, metric, knownAbsent, knownPresent :+ best, path :+ PartitionByInnerNode.PRESENT_KEY)
                val absentSize = absentFolder.diskSize(fs)
                Logger.log("partition-absent", presentSize/threshold.toDouble)
                if (absentSize > threshold)
                    partition(threshold, sampleSize, metric, knownAbsent :+ best, knownPresent, path :+ PartitionByInnerNode.ABSENT_KEY)
            }
        }
    }

    def defrag() = {
        for (bucket <-partitionTree.getBuckets()) {
            val defraggedPartition = PartitionFolder.makeFolder(basePath, false)
            try {
                spark.read
                    .format("parquet")
                    .schema(partitionTree.globalSchema)
                    .load(bucket.folder(basePath).filename)
                    .repartition(1)
                    .write
                    .mode(SaveMode.Overwrite)
                    .format("parquet")
                    .save(defraggedPartition.filename)
                val newNode = Bucket(defraggedPartition.name)
                partitionTree.replace(bucket, newNode)
                writePartitionScheme()
            } catch {
                case e : Throwable => {
                    defraggedPartition.delete(fs)
                    throw e
                }
            }
        }
    }

    def vacuum() = {
        val partitions = partitionTree.getBuckets().map(_.name).toSeq
        println(partitions)
        for (file <- fs.listStatus(new Path(basePath))) {
            val path = file.getPath()
            println(path)
            if (file.isDirectory() && !partitions.contains(path.getName())) {
                println("deleting...")
                fs.delete(path, true)
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
            res ++= buffer.slice(0, read)
        }
        new String(res.toArray, StandardCharsets.UTF_8)
    }
}
