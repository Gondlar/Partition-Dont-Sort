package de.unikl.cs.dbis.waves

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem,Path}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation,TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.TaskContext

import scala.collection.mutable.ArrayBuffer

class WavesRelation(
    override val sqlContext: SQLContext,
    val basePath : Path,
    var globalSchema : StructType,
) extends BaseRelation with Serializable with TableScan {

  private val fs = basePath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
  private val schemaPath = new Path(basePath.toString + WavesRelation.SCHEMA_FILE_NAME)
  private var partitions : Map[String,WavesPartition] = {
    if(fs.exists(schemaPath)) {
      WavesPartition.fromJson(fs.open(schemaPath).readUTF())
    } else {
      Map()
    }
  }

  if (globalSchema == null) {
    globalSchema = partitions.get(WavesRelation.SPILL_PARTITION_NAME)
                             .map(partition => partition.schema)
                             .getOrElse(null)
  }
  assert(sqlContext != null)

  override def schema: StructType = globalSchema

  def getOrCreatePartition(name : String, schema : StructType) : WavesPartition = {
    if (!partitions.contains(name)) {
      partitions += (name -> new WavesPartition(name, schema))
    }
    partitions(name)
  }

  def writePartitionScheme() = {
    val jsonString = WavesPartition.toJson(partitions)
    val out = fs.create(schemaPath)
    out.writeUTF(jsonString)
    out.close()
  }

  def repartition(partition: String, key: String) = {
    val partitionSchema = partitions(partition).schema
    val partitionWithKey = PartitionFolder.makeFolder(basePath.toString(), fs)
    val partitionWithoutKey = PartitionFolder.makeFolder(basePath.toString(), fs)

    val error = sqlContext.sparkContext
                          .runJob(buildScan(), WavesRelation.makeRepartitionJob(partitionSchema,
                                                                                key,
                                                                                partitionWithKey.filename,
                                                                                partitionWithoutKey.filename,
                                                                                sqlContext.sparkContext.hadoopConfiguration))
                          .reduce((lhs, rhs) => lhs.orElse(rhs))
    
    error match {
      case None => {
        partitionWithKey.moveFromTempToFinal(fs)
        partitionWithoutKey.moveFromTempToFinal(fs)
        //TODO write new schema.json
      }
      case Some(exception) => {
        partitionWithKey.delete(fs)
        partitionWithoutKey.delete(fs)
        throw exception
      }
    }
  }

  override def buildScan(): RDD[Row] = {
    if (partitions.isEmpty) sqlContext.sparkContext.emptyRDD[Row];
    else {
      val folders = partitions.values.map(partition => partition.folder(basePath.toString()).filename).toSeq
      sqlContext.sparkSession.read.format("parquet").load(folders:_*).rdd
    }
  }

}

object WavesRelation {
  val SPILL_PARTITION_NAME = "spill"
  val SCHEMA_FILE_NAME = "/schema.json"

  private def makeRepartitionJob(partitionSchema: StructType, key: String, partitionWithKey: String, partitionWithoutKey: String, hadoopConfiguration: Configuration) = {
    val conf = new SerializableConfiguration(hadoopConfiguration)
    (ctx : TaskContext, partition : Iterator[Row]) => {
      val pathKey = PathKey(key)
      var error = Option.empty[Exception]
      try {
        val writerFactory = (name: String) => LocalSchemaWriteSupport.newWriter(partitionSchema,
                                                                      schema => SchemaTransforms.alwaysAbsent(pathKey, schema),
                                                                      conf.value,
                                                                      name,
                                                                      ctx.taskAttemptId().toHexString)

        val missingRows = List.fill(pathKey.maxDefinitionLevel+1)(ArrayBuffer.empty[Row])
        val presentRows = ArrayBuffer.empty[Row]

        for (row <- partition) {
          pathKey.retrieveFrom(row) match {
            case Left(definitionLevel) => missingRows(definitionLevel).append(row)
            case Right(_) => presentRows.append(row)
          }
        }

        val absentPartition = writerFactory(partitionWithoutKey)
        for (group <- missingRows) {
          for (row <- group) {
            absentPartition.write(null, row)
          }
        }

        val presentPartition = writerFactory(partitionWithKey)
        for (row <- presentRows) {
          presentPartition.write(null, row)
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
