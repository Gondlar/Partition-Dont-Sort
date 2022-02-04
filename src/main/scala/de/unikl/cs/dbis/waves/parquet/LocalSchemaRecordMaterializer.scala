package de.unikl.cs.dbis.waves.parquet

import org.apache.parquet.io.api.{RecordMaterializer,GroupConverter}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.conf.Configuration

class LocalSchemaRecordMaterializer private (val root : RowConverter = null) extends RecordMaterializer[Row] {

    private var row : Row = null

    override def getCurrentRecord(): Row = row

    override def getRootConverter(): GroupConverter = root

}

object  LocalSchemaRecordMaterializer {
    def apply(config : Configuration) : LocalSchemaRecordMaterializer = {
        val globalSchema = StructType.fromDDL(config.get(CONFIG_KEY_SCHEMA))
        val materializer = new LocalSchemaRecordMaterializer(RowConverter(globalSchema))
        materializer.root.setPush((result : Any) => {
            materializer.row = result.asInstanceOf[Row]
        })
        materializer
    }

    val CONFIG_KEY_SCHEMA = "waves.globalschema"
}
