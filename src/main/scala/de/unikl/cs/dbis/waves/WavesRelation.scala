package de.unikl.cs.dbis.waves

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem,Path}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation,TableScan,PrunedFilteredScan,Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.TaskContext

import scala.collection.mutable.{ArrayBuffer,ListBuffer}
import java.nio.charset.StandardCharsets

import de.unikl.cs.dbis.waves.partitions.PartitionTree
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.PartitionByInnerNode

class WavesRelation private (
    override val sqlContext: SQLContext,
    val basePath : String,
    private var partitionTree : PartitionTree,
    private val fs : FileSystem,
    private val schemaPath : Path
) extends BaseRelation with Serializable with TableScan with PrunedFilteredScan {
  assert(sqlContext != null)

  override def schema: StructType = partitionTree.globalSchema

  def fastInsertLocation = partitionTree.fastInsertLocation
  def createSpillPartition() = partitionTree.createSpillPartition(() => PartitionFolder.makeFolder(basePath, fs, false).name)

  def writePartitionScheme() = {
    val json = partitionTree.toJson.getBytes(StandardCharsets.UTF_8)
    val out = fs.create(schemaPath)
    out.write(json)
    out.close()
  }

  def repartition(partitionName: String, key: String) = {
    //This is inefficient for very large trees
    val partition = partitionTree.getBuckets().find(bucket => bucket.name == partitionName).get

    val partitionSchema = schema //TODO, I guess
    val partitionWithKey = PartitionFolder.makeFolder(basePath, fs)
    val partitionWithoutKey = PartitionFolder.makeFolder(basePath, fs)
    val newPartitionTree = PartitionByInnerNode(key, partitionWithKey.name, partitionWithoutKey.name)
    partitionTree.replace(partition, newPartitionTree)

    val error = sqlContext.sparkContext
                          .runJob( scanPartition(partition)
                                 , WavesRelation.makeRepartitionJob( partitionSchema
                                                                   , key
                                                                   , partitionWithKey.filename
                                                                   , partitionWithoutKey.filename
                                                                   , sqlContext.sparkContext.hadoopConfiguration))
                          .reduce((lhs, rhs) => lhs.orElse(rhs))
    
    error match {
      case None => {
        partitionWithKey.moveFromTempToFinal(fs)
        partitionWithoutKey.moveFromTempToFinal(fs)
        writePartitionScheme()
      }
      case Some(exception) => {
        partitionWithKey.delete(fs)
        partitionWithoutKey.delete(fs)
        throw exception
      }
    }
  }

  override def buildScan(): RDD[Row] = {
    println("Performing Complete Scan")
    val folders = partitionTree.getBuckets().map(bucket => bucket.folder(basePath).filename).toSeq
    if (folders.isEmpty) {
      sqlContext.sparkContext.emptyRDD[Row];
    } else {
      sqlContext.sparkSession.read.format("parquet").load(folders:_*).rdd
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    println("Performing Partial Scan")
    val folders = partitionTree.getBuckets(filters).map(bucket => bucket.folder(basePath).filename).toSeq
    println(s"Scanning partitions: ${folders.mkString(";")}")
    if (folders.isEmpty) {
      sqlContext.sparkContext.emptyRDD[Row];
    } else {
      sqlContext.sparkSession.read.format("parquet").load(folders:_*).rdd
    }
  }

  private def scanPartition(partition: Bucket) : RDD[Row]
    = scanPartition(partition.folder(basePath))
  private def scanPartition(folder: PartitionFolder): RDD[Row]
    = sqlContext.sparkSession.read.format("parquet").load(folder.filename).rdd

}

object WavesRelation {
  val SPILL_PARTITION_NAME = "spill"
  val SCHEMA_FILE_NAME = "/schema.json"

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

  def apply(sqlContext: SQLContext, basePath: String, globalSchema: StructType) = {
    val fs = new Path(basePath).getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val schemaPath = new Path(basePath + SCHEMA_FILE_NAME)
    val partitionTree = if (fs.exists(schemaPath)) {
        assert(globalSchema == null)
        val schema = readSchema(schemaPath, fs)
        println(s"'${schema.last.toHexString}'")
        PartitionTree.fromJson(schema)
      } else {
        new PartitionTree(globalSchema)
      }
    new WavesRelation(sqlContext, basePath, partitionTree, fs, schemaPath)
  }

  private def makeRepartitionJob(partitionSchema: StructType, key: String, partitionWithKey: String, partitionWithoutKey: String, hadoopConfiguration: Configuration) = {
    val conf = new SerializableConfiguration(hadoopConfiguration)
    (ctx : TaskContext, partition : Iterator[Row]) => {
      val pathKey = PathKey(key)
      var error = Option.empty[Exception]
      try {
        val writerFactory = (name: String) => LocalSchemaWriteSupport.newWriter(partitionSchema,
                                                                      //schema => SchemaTransforms.alwaysAbsent(pathKey, schema),
                                                                      schema => schema,
                                                                      conf.value,
                                                                      name,
                                                                      ctx.taskAttemptId().toHexString)

        val absentPartition = writerFactory(partitionWithoutKey)
        val presentPartition = writerFactory(partitionWithKey)

        for (row <- partition) {
          pathKey.retrieveFrom(row) match {
            case Left(definitionLevel) => absentPartition.write(null, row)
            case Right(_) => presentPartition.write(null, row)
          }
        }

        absentPartition.close(null)
        presentPartition.close(null)

      } catch {
        case e: Exception => {
          error = Some(e)
        }
      }
      error
    }
  }
}
