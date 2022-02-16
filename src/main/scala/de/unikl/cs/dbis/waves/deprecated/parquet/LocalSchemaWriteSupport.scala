package de.unikl.cs.dbis.waves.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{MessageType,GroupType,Type}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,DataType,ArrayType}
import org.apache.spark.sql.execution.datasources.parquet.SparkToParquetSchemaConverter

import java.util.HashMap
import org.apache.commons.lang.NotImplementedException
import org.apache.parquet.ShouldNeverHappenException

class LocalSchemaWriteSupport(
    val globalSchema : StructType,
    val localSchema : StructType
) extends WriteSupport[Row] {
    def this(schema : StructType) = this(schema, schema)

    private var parquetSchema : MessageType = null
    private var out : RecordConsumer = null

    def init(configuration: Configuration): WriteSupport.WriteContext = {
        val converter = new SparkToParquetSchemaConverter()
        parquetSchema = converter.convert(localSchema)

        //TODO write global/local schema to file as extra meta data?
        val extraMetaData : HashMap[String,String] = new HashMap[String,String]()
        new WriteSupport.WriteContext(parquetSchema, extraMetaData)
    }

    def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
        out = recordConsumer
    }

    def write(record: Row): Unit = {
        assert(out != null)
        assert(parquetSchema != null)

        out.startMessage()
        writeRow(record, globalSchema, localSchema, parquetSchema) //TODO work with local schema
        out.endMessage()
    }

    private def writeRow(record: Row, globalSchema : StructType, localSchema : StructType, parquetSchema : GroupType) : Unit = {
        // Record has the global scheme, we want to write the local scheme to parquet

        val size = globalSchema.size
        assert(size == record.size)
        val parquetFields = parquetSchema.getFields()
        assert(parquetFields.size == localSchema.fields.size)

        var localIndex = 0 // Local schema has fewer elements than global schema
        for (index <- 0 to size-1) {
            if (localSchema.fields(localIndex).name == globalSchema.fields(index).name) {
                val itemParquetType = parquetFields.get(localIndex)
                val item = record.get(index)
                val itemType = localSchema.fields(localIndex)
                val globalType = globalSchema.fields(index)
                if(item != null) {
                    val name = itemParquetType.getName()
                    out.startField(name, localIndex)
                    writeField(item, globalType.dataType, itemType.dataType, itemParquetType)
                    out.endField(name, localIndex)
                } else if (!itemType.nullable) throw new RuntimeException(s"Field '${itemType.name}' is null but not nullable")

                localIndex += 1
            }
        }
        assert(localIndex == localSchema.size)
    }

    private def writeArray(list: Iterator[Any], globalType: ArrayType, valueType: ArrayType, parquetType: GroupType) = {
        val elementType = parquetType.getType(0).asGroupType().getType(0)
        if (list.hasNext) {
            out.startField(LocalSchemaWriteSupport.LIST_NAME, 0)
            for (row <- list) {
                out.startGroup()
                if (row != null) {
                    out.startField(LocalSchemaWriteSupport.LIST_ELEMENT_NAME, 0)
                    writeField(row, globalType.elementType, valueType.elementType, elementType)
                    out.endField(LocalSchemaWriteSupport.LIST_ELEMENT_NAME, 0)
                } else if (!valueType.containsNull) throw new RuntimeException("Null value in non-nullable array entry")
                out.endGroup()
            }
            out.endField(LocalSchemaWriteSupport.LIST_NAME, 0)
        }
    }

    private def writeField(value : Any, globalType : DataType, valueType : DataType, parquetType : Type) : Unit = {
        value match {
            case str : String => {
                assert(valueType.typeName.equals("string"))
                assert(globalType.typeName.equals("string"))
                out.addBinary(Binary.fromString(str))
            }
            case l : Long => {
                assert(valueType.typeName.equals("long"))
                assert(globalType.typeName.equals("long"))
                out.addLong(l)
            }
            case d : Double => {
                assert(valueType.typeName.equals("double"))
                assert(globalType.typeName.equals("double"))
                out.addDouble(d)
            }
            case b : Boolean => {
                assert(valueType.typeName.equals("boolean"))
                assert(globalType.typeName.equals("boolean"))
                out.addBoolean(b)
            }
            case row : Row => {
                assert(valueType.typeName.equals("struct"))
                assert(globalType.typeName.equals("struct"))
                out.startGroup()
                writeRow(row, globalType.asInstanceOf[StructType], valueType.asInstanceOf[StructType],  parquetType.asGroupType)
                out.endGroup()
            }
            case list : Iterable[Any] => {
                assert(valueType.typeName.equals("array"))
                assert(globalType.typeName.equals("array"))
                out.startGroup()
                writeArray(list.iterator, globalType.asInstanceOf[ArrayType], valueType.asInstanceOf[ArrayType], parquetType.asGroupType)
                out.endGroup()
            }
            case other => throw new NotImplementedException(valueType.typeName) //TODO implement missing types
        }
    }
}

object LocalSchemaWriteSupport {
    val LIST_NAME = "array"
    val LIST_ELEMENT_NAME = "element"

    def newWriter(schema: StructType, transform: StructType => StructType, config: Configuration, basePath: String, id: String)
        = new ParquetOutputFormat[Row](new LocalSchemaWriteSupport(schema, transform(schema)))
            .getRecordWriter(config, new Path(s"$basePath/part-$id.snappy.parquet"), CompressionCodecName.SNAPPY)
}
