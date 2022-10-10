package de.unikl.cs.dbis.waves.sort

import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.util.operators.Grouper
import java.lang.reflect.Type
import com.google.gson.{
  JsonDeserializer, JsonDeserializationContext,
  JsonSerializer, JsonSerializationContext,
  JsonElement, JsonObject, JsonPrimitive, JsonParseException
}

trait Sorter {

  /**
    * @return this sorter's name. It is used for serialization
    */
  def name: String

  /**
    * Sort the data within a bucket
    *
    * @param bucket the bucket grouped by [[sortGrouper]]
    * @return the sorted bucket
    */
  def sort(bucket: DataFrame): DataFrame

  /**
    * A Grouper to group the data by. Each grouping represents one kind of data
    * This grouper is used when sorting the data.
    */
  def grouper: Grouper
}

object SorterDeserializer extends JsonDeserializer[Sorter] {

  val KIND_KEY = "name"

  override def deserialize(json: JsonElement, typeOfT: Type, ctx: JsonDeserializationContext): Sorter = {
    json match {
      case obj: JsonObject => obj.get(KIND_KEY).getAsString().toLowerCase match {
        case NoSorter.name => NoSorter
        case LexicographicSorter.name => LexicographicSorter
        case unknown => throw new JsonParseException(s"kind '$unknown' is unknown")
      }
      case primitive: JsonPrimitive => primitive.getAsString().toLowerCase match {
        case NoSorter.name => NoSorter
        case LexicographicSorter.name => LexicographicSorter
        case unknown => throw new JsonParseException(s"kind '$unknown' is unknown")
      }
      case _ => throw new JsonParseException(s"$json is not an object")
    }
  }
}
