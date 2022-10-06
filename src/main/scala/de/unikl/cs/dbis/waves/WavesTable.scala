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
    PartitionTree,TreeNode,SplitByPresence,Spill,Bucket,PartitionTreePath,PartitionTreeHDFSInterface
}
import de.unikl.cs.dbis.waves.partitions.visitors.CollectBucketsVisitor
import de.unikl.cs.dbis.waves.split.PredefinedSplitter
import de.unikl.cs.dbis.waves.util.{PathKey,Logger, PartitionFolder}

import java.{util => ju}
import collection.JavaConverters._
import org.apache.spark.sql.DataFrameReader

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

    /**
      * Replace the subtree (or Bucket) at the given path with a new split by
      * the given key
      *
      * @param key the key to split by
      * @param path the path where the split should be inserted
      */
    def split(key: String, path : PartitionTreePath *) : Unit = {
        val newSplit = SplitByPresence(key, s"$key-absent", s"$key-present")
        repartition(path, newSplit)
    }

    /**
      * Move all data from Buckets into appropriate Buckets and delete the
      * Spill nodes from the tree.
      * 
      * Caution: Some splitters, e.g., the [[RandomSplitter]], use Spill nodes
      * to create multiple Buckets without implying differences in the data. If
      * you use such a splitter, do not call unspill as it will undo the
      * partitioning.
      */
    def unspill: Unit = {
      // Find all data in spill buckets
      val spillBucketPaths = partitionTree.metadata.filter(_.isSpillBucket).map(_.getPath)
      val hasNonEmptySpillBuckets = spillBucketPaths
        .map(p => partitionTree.find(p).get.asInstanceOf[Bucket[String]].folder(basePath))
        .filter(!_.isEmpty(fs))
        .nonEmpty
      if (hasNonEmptySpillBuckets) {
        // repartition all data into the desired partitioning scheme
        val data = spark.read.parquet(findRequiredPartitions(Seq.empty).map(_.filename):_*)
        new PredefinedSplitter(partitionTree.root, Seq.empty).prepare(data, basePath).partition()
        partitionTree = hdfsInterface.read().get
      }

      // remove the now empty spill buckets
      for (path <- spillBucketPaths) {
        val pathToSpillNode = path.init
        val theSpillNode = partitionTree.find(pathToSpillNode).get.asInstanceOf[Spill[String]]
        partitionTree.replace(pathToSpillNode, theSpillNode.partitioned)
      }
      writePartitionScheme()
    }

    /**
      * Change the partitioning of a given subtree to the given shape
      *
      * @param path the path to the root of the subtree to repartition
      * @param shape the shape of the new partitioning, i.e., the resulting
      *              table will have a partitioning scheme with the same kinds
      *              of nodes, but the names of the Buckets may differ
      */
    def repartition(path: Seq[PartitionTreePath], shape: TreeNode.AnyNode[String]) = {
        val df = partitionTree.find(path)
                              .get(new CollectBucketsVisitor[String]())
                              .map(b => spark.read.parquet(b.folder(basePath).filename))
                              .reduce((lhs, rhs) => lhs.union(rhs))
        new PredefinedSplitter(shape, path).prepare(df, basePath).partition()
        partitionTree = hdfsInterface.read().get
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
    val PACKAGE = "de.unikl.cs.dbis.waves"

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
        writer.format(PACKAGE)
              .option(PARTITION_TREE_OPTION, new PartitionTree(schema).toJson)
              .save(path)
      }
    }

    implicit class reader(reader: DataFrameReader) {
      def waves(path: String) = reader.format(PACKAGE).load(path)
    }
}
