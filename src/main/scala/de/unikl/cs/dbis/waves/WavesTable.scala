package de.unikl.cs.dbis.waves

import org.apache.hadoop.fs.{Path,FileSystem}

import org.apache.spark.sql.{SparkSession,SaveMode,Row,DataFrame}
import org.apache.spark.sql.connector.catalog.{Table,TableCapability,SupportsRead,SupportsWrite}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo,WriteBuilder,SupportsTruncate}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.DataFrameWriter

import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

import de.unikl.cs.dbis.waves.partitions.{
    PartitionTree,SplitByPresence,Bucket,PartitionTreePath,Present,Absent,PartitionTreeHDFSInterface
}
import de.unikl.cs.dbis.waves.util.{PathKey,Logger, PartitionFolder}

import java.{util => ju}
import collection.JavaConverters._

class WavesTable private (
    override val name : String,
    val spark : SparkSession,
    val basePath : String,
    val fs : FileSystem,
    options : CaseInsensitiveStringMap,
    private[waves] var partitionTree : PartitionTree[String]
) extends Table with SupportsRead with SupportsWrite {
    private val fastWrite = options.getBoolean(WavesTable.FAST_WRITE_OPTION, true)

    private val hdfsInterface = PartitionTreeHDFSInterface(fs, basePath)

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
    
    private[waves] def writePartitionScheme() = hdfsInterface.write(partitionTree)

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

    private def repartition(key: String, partition : Bucket[String]) : Unit = {
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
        val partitions = partitionTree.getBuckets().map(_.data).toSeq :+ "spill" // leave initial spill partition for benchmark purposes
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

    def apply(name : String, spark : SparkSession, basePath : String, options : CaseInsensitiveStringMap) = {
        val hdfsInterface = PartitionTreeHDFSInterface(spark, basePath)
        val schemaOption = options.get(PARTITION_TREE_OPTION)
        val partitionTree = if (schemaOption != null) {
            // partition tree overrides schema
            PartitionTree.fromJson(schemaOption)
        } else {
          hdfsInterface.read() match {
            case Some(tree) => tree
            case None => throw QueryCompilationErrors.dataSchemaNotSpecifiedError("waves")
          }
        }
        new WavesTable(name, spark, basePath, hdfsInterface.fs, options, partitionTree)
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

    implicit class implicits(df: DataFrame) {

      /**
        * Retrieve the WavesTable the DataFrame reads from.
        * A DataFrame is considered to read from a WavesTable if it is just a
        * plain scan on that table.
        *
        * @return the WaveTable or None if no such table exists
        */
      def getWavesTable: Option[WavesTable] = df.queryExecution.logical match {
        case relation: DataSourceV2Relation => relation.table match {
          case table: WavesTable => Some(table)
          case _ => None
        }
        case _ => None
      }

      /**
        * @return true iff the DataFrame reads from a WavesTable
        * @see [[getWavesTable]]
        */
      def isWavesTable: Boolean = getWavesTable.nonEmpty
    }

    implicit class writer(writer: DataFrameWriter[Row]) {

      /**
        * Write the dataframe as a Waves table.
        * 
        * Do not use `.format(...).save(path)` manually, it will produce wrong
        * results
        *
        * @param path the path to write to
        * @param schema the DataFrame's schema. Yes we need it again, don't ask.
        */
      def waves(path: String, schema: StructType) = {
        writer.format("de.unikl.cs.dbis.waves")
              .option(PARTITION_TREE_OPTION, new PartitionTree(schema).toJson)
              .save(path)
      }
    }
}
