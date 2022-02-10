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
        val projectedSchema = loadProjectedSchema(config, globalSchema).getOrElse(globalSchema)
        val materializer = new LocalSchemaRecordMaterializer(RowConverter(projectedSchema))
        materializer.root.setPush((result : Any) => {
            materializer.row = result.asInstanceOf[Row]
        })
        materializer
    }

    def loadProjectedSchema(config : Configuration) : Option[StructType] = {
        val globalSchema = StructType.fromDDL(config.get(CONFIG_KEY_SCHEMA))
        loadProjectedSchema(config, globalSchema)
    }

    def loadProjectedSchema(config : Configuration, globalSchema : StructType) : Option[StructType] = {
        if (config.get(LocalSchemaRecordMaterializer.CONFIG_KEY_PROJECTION) != null) {
            val indices = config.getInts(LocalSchemaRecordMaterializer.CONFIG_KEY_PROJECTION)
            Some(StructType(indices.map(index => globalSchema(index))))
        } else None
    }

    val CONFIG_KEY_SCHEMA = "waves.globalschema"
    val CONFIG_KEY_PROJECTION = "waves.projection"
}
