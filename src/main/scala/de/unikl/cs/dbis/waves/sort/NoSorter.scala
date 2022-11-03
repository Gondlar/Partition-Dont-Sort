package de.unikl.cs.dbis.waves.sort

import org.apache.spark.sql.DataFrame

import de.unikl.cs.dbis.waves.split.IntermediateData
import de.unikl.cs.dbis.waves.util.operators.Grouper
import de.unikl.cs.dbis.waves.util.operators.NullGrouper

import java.lang.reflect.Type
import com.google.gson.{
  JsonSerializer,JsonSerializationContext,JsonElement,JsonPrimitive
}

/**
  * Sorter for when we do not wish to sort
  */
object NoSorter extends Sorter {
  override val name = "none"
  override def sort(bucket: IntermediateData): IntermediateData = bucket
  override def grouper: Grouper = NullGrouper
}

object NoSorterSerializer extends JsonSerializer[NoSorter.type] {
  override def serialize(src: NoSorter.type, typeOfSrc: Type, ctx: JsonSerializationContext): JsonElement
    = new JsonPrimitive(NoSorter.name)
}
