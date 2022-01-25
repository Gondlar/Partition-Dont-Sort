package de.unikl.cs.dbis.waves

import org.apache.hadoop.shaded.com.google.gson.{
    JsonDeserializer,
    JsonSerializer,
    JsonElement,
    JsonObject,
    JsonArray,
    JsonDeserializationContext,
    JsonSerializationContext,
    JsonParseException,
    GsonBuilder
}
import java.lang.reflect.Type
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType

class WavesPartition(
    val name : String,
    val schema : StructType
) {
    def folder(basePath : String) = new PartitionFolder(basePath, name, false)
}

object WavesPartitionSerializer extends JsonSerializer[Map[String,WavesPartition]] {

  override def serialize(map: Map[String,WavesPartition], t: Type, ctx: JsonSerializationContext): JsonElement = {
      val arr = new JsonArray()
      for (partition <- map.values) {
          val obj = new JsonObject()
          obj.addProperty("name", partition.name)
          obj.addProperty("schema", partition.schema.toDDL)
          arr.add(obj)
      }
      return arr
  }

}

object WavesPartitionDeserializer extends JsonDeserializer[Map[String,WavesPartition]] {

  override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): Map[String,WavesPartition] = {
      json match {
          case arr: JsonArray => {
              var res = Map[String,WavesPartition]()
              for (index <- 0 to arr.size()-1) {
                  arr.get(index) match {
                      case o: JsonObject if o.has("name") => {
                          val name = o.get("name").getAsString()
                          val schema = StructType.fromDDL(o.get("schema").getAsString())
                          res += (name -> new WavesPartition(name, schema))
                      }
                      case _ => throw new JsonParseException(s"$json is not an array of Partitions")
                  }
              }
              return res
          }
          case _ => throw new JsonParseException(s"$json is not an array of Partitions")
      }
  }

}

object WavesPartition {
    private val gson = new GsonBuilder().registerTypeHierarchyAdapter(classOf[Map[String,WavesPartition]], WavesPartitionDeserializer)
                                        .registerTypeHierarchyAdapter(classOf[Map[String,WavesPartition]], WavesPartitionSerializer)
                                        .create()

    def toJson(partitions : Map[String,WavesPartition]) : String = {
        val str = gson.toJson(partitions)
        println(str)
        return str
    }

    def fromJson(str : String) : Map[String,WavesPartition] = {
        gson.fromJson(str, classOf[Map[String,WavesPartition]])
    }
}
