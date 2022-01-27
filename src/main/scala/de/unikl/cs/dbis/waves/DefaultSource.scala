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
          case Some(basePath) => WavesRelation(sqlContext, basePath, schema)
      }
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String,String], data: DataFrame): BaseRelation = {
      val baseDir = parameters.getOrElse("path", {
          throw new IllegalArgumentException("No Basepath specified");
      })
      val path = new Path(baseDir)
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

      var relation = WavesRelation(sqlContext, baseDir, data.schema)
      val insertLocation = relation.fastInsertLocation.get //TODO create spill if necessary
      relation.writePartitionScheme();
      data.repartition(1)
          .write
          .mode(mode)
          //.option("maxRecordsPerFile", 100000) //TODO proper estimate for records per file
          .parquet(insertLocation.folder(baseDir).filename)
      relation
  }
  
}
