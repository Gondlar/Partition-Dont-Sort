package de.unikl.cs.dbis.waves.parquet

import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import de.unikl.cs.dbis.waves.PartitionFolder
import org.apache.spark.sql.types.StructType

class LocalSchemaInputFormat
extends ParquetInputFormat[Row](classOf[LocalScheaReadSupport])

object LocalSchemaInputFormat {
    def read(sc : SparkContext, globalSchema : StructType, folder : PartitionFolder) : RDD[Row] = {
        val conf = new Configuration(sc.hadoopConfiguration)
        conf.set(LocalSchemaRecordMaterializer.CONFIG_KEY_SCHEMA, globalSchema.toDDL)
        //TODO predicate pushdown
        sc.newAPIHadoopFile( folder.filename
                           , classOf[LocalSchemaInputFormat]
                           , classOf[Void]
                           , classOf[Row]
                           , conf)
          .values
    }
}
