package de.unikl.cs.dbis.waves

import org.apache.hadoop.fs.{Path,FileSystem}

import org.apache.spark.sql.{SparkSession,SaveMode,Row,DataFrame}
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

import de.unikl.cs.dbis.waves.partitions.{
    PartitionTree,SplitByPresence,Bucket,PartitionTreePath,Present,Absent
}
import de.unikl.cs.dbis.waves.util.{PathKey,Logger, PartitionFolder}

import java.{util => ju}
import java.nio.charset.StandardCharsets
import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class WavesTable private (
    override val name : String,
    val spark : SparkSession,
    val basePath : String,
    val fs : FileSystem,
    options : CaseInsensitiveStringMap,
    private[waves] var partitionTree : PartitionTree
) extends Table with SupportsRead with SupportsWrite {
    private val fastWrite = options.getBoolean(WavesTable.FAST_WRITE_OPTION, true)

    override def capabilities(): ju.Set[TableCapability]
        = Set( TableCapability.BATCH_READ
             , TableCapability.BATCH_WRITE
             , TableCapability.TRUNCATE
             ).asJava

    override def schema(): StructType = partitionTree.globalSchema

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder
        = new WavesScanBuilder(this, options)
    
    override def newWriteBuilder(options: LogicalWriteInfo): WriteBuilder
        = if (fastWrite) WavesFastWriteBuilder(this, options)
          else null //TODO slow write

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
        = partitionTree.findOrCreateFastInsertLocation(() => PartitionFolder.makeFolder(basePath, false).name)
    
    private[waves] def writePartitionScheme() = {
        val json = partitionTree.toJson.getBytes(StandardCharsets.UTF_8)
        val out = fs.create(WavesTable.makeSchemaPath(basePath))
        out.write(json)
        out.close()
    }

    private[waves] def insertLocation() = createSpillPartition().folder(basePath)
    
    private[waves] def truncate() = {
        partitionTree = new PartitionTree(schema)
        insertLocation()
    }

    def diskSize() : Long = {
        val foo = partitionTree.getBuckets()
        foo.map(_.folder(basePath).diskSize(fs)).sum
    }

    def repartition(key: String, path : PartitionTreePath *) : Unit = {
        // Find partition
        val partition = partitionTree.find(path) match {
            case Some(bucket@Bucket(_)) => bucket
            case _ => throw new RuntimeException("Partition not found")
        }
        repartition(key, partition)
    }

    private def repartition(key: String, partition : Bucket) : Unit = {
        // Modify partition tree
        val partitionWithKey = PartitionFolder.makeFolder(basePath, false)
        val partitionWithoutKey = PartitionFolder.makeFolder(basePath, false)
        val newNode = SplitByPresence(key, partitionWithKey.name, partitionWithoutKey.name)
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
        writePartitionScheme()
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
        val partitions = partitionTree.getBuckets().map(_.name).toSeq :+ "spill" // leave initial spill partition for benchmark purposes
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
    /**
      * Key for the partition tree option.
      * If it is set, it must contan a JSON serialized partition tree to use with the Table. Compatibility with the given schema is
      * not checked, use with caution.
      */
    val PARTITION_TREE_OPTION = "waves.schema"

    val FAST_WRITE_OPTION = "waves.fastWrite"

    def makeSchemaPath(basePath : String) = new Path(s"$basePath/schema.json")

    def apply(name : String, spark : SparkSession, basePath : String, options : CaseInsensitiveStringMap) = {
        val schemaPath = makeSchemaPath(basePath)
        val fs = schemaPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val schemaOption = options.get(PARTITION_TREE_OPTION)
        val partitionTree = if (schemaOption != null) {
            // partition tree overrides schema
            PartitionTree.fromJson(schemaOption)
        } else if (fs.exists(schemaPath)) {
            PartitionTree.fromJson(readSchema(schemaPath, fs))
        } else {
            throw QueryCompilationErrors.dataSchemaNotSpecifiedError("waves")
        }
        new WavesTable(name, spark, basePath, fs, options, partitionTree)
    }

    def apply(name : String, spark : SparkSession, basePath : String, options : CaseInsensitiveStringMap, schema : StructType) = {
        val schemaOption = options.get(PARTITION_TREE_OPTION)
        val partitionTree = if (schemaOption != null) {
            // partition tree overrides schema
            PartitionTree.fromJson(schemaOption)
        } else {
            new PartitionTree(schema)
        }
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
