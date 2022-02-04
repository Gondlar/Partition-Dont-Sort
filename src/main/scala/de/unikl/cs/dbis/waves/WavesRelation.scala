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

import de.unikl.cs.dbis.waves.partitions.{PartitionTree,Bucket,PartitionByInnerNode}
import de.unikl.cs.dbis.waves.parquet.{LocalSchemaInputFormat,LocalSchemaWriteSupport}
import de.unikl.cs.dbis.waves.util.{PathKey,Logger}
import de.unikl.cs.dbis.waves.util.ValByNeed

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
    Logger.log("complete-scan")
    val res = scanPartition(partitionTree.getBuckets().toSeq:_*)
    Logger.log("complete-scan-built")
    res
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    Logger.log("partial-scan")
    val res = scanPartition(partitionTree.getBuckets(filters).toSeq:_*)
    Logger.log("partial-scan-built")
    res
  }

  private def scanPartition(partitions: Bucket*) : RDD[Row]
    = scanFolder(partitions.map(bucket => bucket.folder(basePath)):_*)

  private def scanFolder(folders: PartitionFolder*) : RDD[Row] = {
    Logger.log("chose-buckets", folders.mkString(";"))
    val rdds = folders.map(folder => LocalSchemaInputFormat.read(sqlContext.sparkContext, schema, folder))
    rdds.length match {
      case 0 => sqlContext.sparkContext.emptyRDD[Row]
      case 1 => rdds(0)
      case _ => sqlContext.sparkContext.union(rdds)
    }
  }

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
        PartitionTree.fromJson(readSchema(schemaPath, fs))
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

        val absentPartition = new ValByNeed(() => writerFactory(partitionWithoutKey))
        val presentPartition = new ValByNeed(() => writerFactory(partitionWithKey))

        for (row <- partition) {
          pathKey.retrieveFrom(row) match {
            case Left(definitionLevel) => absentPartition.modify(v => v.write(null, row))
            case Right(_) => presentPartition.modify(v => v.write(null, row))
          }
        }

        absentPartition.optionalModify(v => v.close(null))
        presentPartition.optionalModify(v => v.close(null))

      } catch {
        case e: Exception => {
          error = Some(e)
        }
      }
      error
    }
  }
}
