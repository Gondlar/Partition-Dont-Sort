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
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.DataFrameWriter

import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

import de.unikl.cs.dbis.waves.partitions.{
    PartitionTree,TreeNode,SplitByPresence,Spill,Bucket,PartitionTreePath,PartitionTreeHDFSInterface
}
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.sort.{Sorter,NoSorter}
import de.unikl.cs.dbis.waves.split.{Splitter,PredefinedSplitter}
import de.unikl.cs.dbis.waves.util.{PathKey,Logger, PartitionFolder}

import java.{util => ju}
import collection.JavaConverters._
import org.apache.spark.sql.DataFrameReader

import TreeNode.AnyNode
import PartitionTree._

class WavesTable private (
    override val name : String,
    val spark : SparkSession,
    val basePath : String,
    val fs : FileSystem,
    options : CaseInsensitiveStringMap,
    private[waves] var partitionTree : PartitionTree[String]
) extends Table with SupportsRead with SupportsWrite {
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
        = WavesFastWriteBuilder(this, options)

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
        = partitionTree.bucketsWith(filters)
                       .map(bucket => bucket.folder(basePath))
                       .toSeq
    
    private[waves] def createSpillPartition()
        = partitionTree.findOrCreateFastInsertLocation(() => PartitionFolder.makeFolder(basePath, false).name)
    
    private[waves] def writePartitionScheme() = hdfsInterface.write(partitionTree)

    private[waves] def insertLocation() = createSpillPartition().folder(basePath)
    
    private[waves] def truncate() = {
        partitionTree = new PartitionTree(schema, partitionTree.sorter)
        insertLocation()
    }

    def diskSize() : Long = {
        val foo = partitionTree.buckets
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
      val spillBucketPaths = partitionTree.metadata().filter(_.isSpillBucket).map(_.getPath)
      val hasNonEmptySpillBuckets = spillBucketPaths
        .map(p => partitionTree.find(p).get.asInstanceOf[Bucket[String]].folder(basePath))
        .filter(!_.isEmpty(fs))
        .nonEmpty
      if (hasNonEmptySpillBuckets) {
        // repartition all data into the desired partitioning scheme
        val data = spark.read.parquet(findRequiredPartitions(Seq.empty).map(_.filename):_*)
        new PredefinedSplitter(partitionTree.root, Seq.empty)
          .sortWith(partitionTree.sorter)
          .prepare(data, basePath)
          .partition()
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
                              .get
                              .buckets
                              .map(b => spark.read.parquet(b.folder(basePath).filename))
                              .reduce((lhs, rhs) => lhs.union(rhs))
        new PredefinedSplitter(shape, path)
          .sortWith(partitionTree.sorter)
          .prepare(df, basePath)
          .partition()
        partitionTree = hdfsInterface.read().get
    }

    /**
      * repartition the entire table according to the given splitter
      *
      * @param splitter the splitter
      */
    def repartition(splitter: Splitter[_]): Unit = {
      val df = partitionTree.buckets
                            .map(b => spark.read.parquet(b.folder(basePath).filename))
                            .reduce((lhs, rhs) => lhs.union(rhs))
      splitter.prepare(df, basePath).partition()
    }

    def defrag() = {
        for (bucket <-partitionTree.buckets) {
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
        val partitions = partitionTree.buckets.map(_.data).toSeq :+ "spill" // leave initial spill partition for benchmark purposes
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
      * If it is set, it must contain a JSON serialized AnyNode to use with the
      * Table. Compatibility with the data on disk is not checked, so use with
      * caution.
      */
    val PARTITION_OPTION = "waves.partitions"

    /**
      * Key for the schema option.
      * If it is set, it must contain a JSON serialized StructType to use with
      * the Table. Compatibility with the data on disk is not checked, so use
      * with caution.
      */
    val SCHEMA_OPTION = "waves.schema"

    /**
      * Key for the sort option.
      * If it is set, it must contain a JSON serialized Sorter to use with
      * the Table. Compatibility with the data on disk is not checked, so use
      * with caution.
      */
    val SORT_OPTION = "waves.sort"

    def apply(name : String, spark : SparkSession, basePath : String, options : CaseInsensitiveStringMap, schema : StructType): WavesTable
      = apply(name, spark, basePath, options, Some(schema))

    def apply(name : String, spark : SparkSession, basePath : String, options : CaseInsensitiveStringMap, schema: Option[StructType] = None) = {
      val hdfsInterface = PartitionTreeHDFSInterface(spark, basePath)
      val (schemaOption, sorter, partitionOption) = parseOptions(options)
      val partitionTree = buildTree(schemaOption.orElse(schema), sorter, partitionOption, hdfsInterface)
      new WavesTable(name, spark, basePath, hdfsInterface.fs, options, partitionTree)
    }

    private def parseOptions(options: CaseInsensitiveStringMap) = {
      val schema = Option(options.get(SCHEMA_OPTION)).map(DataType.fromJson(_).asInstanceOf[StructType])
      val sorter = Option(options.get(SORT_OPTION)).map(PartitionTree.sorterFromJson(_))
      val partitions = Option(options.get(PARTITION_OPTION)).map(PartitionTree.treeFromJson(_))
      (schema, sorter, partitions)
    }

    private def buildTree(
      forceSchema: Option[StructType],
      forceSorter: Option[Sorter],
      forcePartitions: Option[AnyNode[String]],
      hdfs: PartitionTreeHDFSInterface
    ): PartitionTree[String] = {
      // if all options are forced, just build the tree
      if (forceSchema.nonEmpty && forcePartitions.nonEmpty && forceSorter.nonEmpty) {
        return new PartitionTree(forceSchema.get, forceSorter.get, forcePartitions.get)
      }
      // load schema from disk
      val (diskSchema, diskSorter, diskPartitions) = hdfs.read() match {
        case None => (None, None, None)
        case Some(diskTree) => (Some(diskTree.globalSchema), Some(diskTree.sorter), Some(diskTree.root))
      }
      // set options in the order force > disk > default
      val schema = forceSchema.orElse(diskSchema).getOrElse(throw QueryCompilationErrors.dataSchemaNotSpecifiedError("waves"))
      val sorter = forceSorter.orElse(diskSorter).getOrElse(NoSorter)
      val partitions = forcePartitions.orElse(diskPartitions).getOrElse(Bucket("spill"))
      new PartitionTree(schema, sorter, partitions)
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

      /**
        * Save the DataFrame as a WavesTable.
        * 
        * This method immediately partitions the data using to the given
        * splitter. This can take a while. If you need to write the data quickly
        * and partition it later, use .save.waves(path) instead.
        *
        * @param splitter the splitter deciding the partitioning
        * @param path the location for saving the data
        */
      def saveAsWaves(splitter: Splitter[_], path: String): Unit
        = splitter.prepare(df, path).partition()
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
      def waves(path: String, schema: StructType)
        = writer.format(PACKAGE)
                .option(SCHEMA_OPTION, schema.json)
                .save(path)

      def sorter(sorter: Sorter) = writer.option(SORT_OPTION, sorter.toJson)

      def partition(tree: AnyNode[String])
        = writer.option(PARTITION_OPTION, tree.toJson)
    }

    implicit class reader(reader: DataFrameReader) {
      def waves(path: String) = reader.format(PACKAGE).load(path)
    }
}
