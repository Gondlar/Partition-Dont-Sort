package de.unikl.cs.dbis.waves.parquet

import org.apache.parquet.hadoop.api.{ReadSupport,InitContext}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.MessageType

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import java.util.Map
import collection.JavaConverters._

class LocalScheaReadSupport extends ReadSupport[Row] {

    override def init(ctx: InitContext): ReadSupport.ReadContext
        = new ReadSupport.ReadContext(ctx.getFileSchema()) //TODO prune columns?

    override def prepareForRead(conf: Configuration,
                                metadata: Map[String,String],
                                fileSchema: MessageType, 
                                ctx: ReadSupport.ReadContext): RecordMaterializer[Row]
        = LocalSchemaRecordMaterializer(conf)
}
