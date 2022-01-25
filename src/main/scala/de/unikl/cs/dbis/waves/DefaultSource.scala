package de.unikl.cs.dbis.waves

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.sources.{RelationProvider, BaseRelation, SchemaRelationProvider,CreatableRelationProvider}
import org.apache.spark.sql.{SQLContext,SaveMode,DataFrame}
import org.apache.spark.sql.types.StructType

class DefaultSource extends RelationProvider
                    with SchemaRelationProvider
                    with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String,String]): BaseRelation
        = createRelation(sqlContext, parameters, null)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String,String], schema: StructType): BaseRelation = {
      parameters.get("path") match {
          case None => throw new IllegalArgumentException("No Basepath specified")
          case Some(basePath) => new WavesRelation(sqlContext, new Path(basePath), schema)
      }
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String,String], data: DataFrame): BaseRelation = {
      val path = new Path(parameters.getOrElse("path", {
          throw new IllegalArgumentException("No Basepath specified");
      }))
      val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
      if (fs.exists(path)) {
          mode match {
              case SaveMode.Overwrite => fs.delete(path, true)
              case SaveMode.ErrorIfExists => sys.error("Path already exists: " + path)
              case SaveMode.Ignore => sys.exit()
              case SaveMode.Append => {}
          }
      } else {
          fs.mkdirs(path)
      }

      var relation = new WavesRelation(sqlContext, path, data.schema)
      val spillPartition = relation.getOrCreatePartition(WavesRelation.SPILL_PARTITION_NAME, data.schema)
      relation.writePartitionScheme();
      data.repartition(1)
          .write
          .mode(mode)
          //.option("maxRecordsPerFile", 100000) //TODO proper estimate for records per file
          .parquet(spillPartition.folder(path.toString()).filename)
      relation
  }
  
}
