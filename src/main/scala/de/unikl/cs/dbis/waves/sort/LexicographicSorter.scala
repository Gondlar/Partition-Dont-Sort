package de.unikl.cs.dbis.waves.sort

import de.unikl.cs.dbis.waves.util.operators.{Grouper, DefinitionLevelGrouper}
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{count_distinct,size}

import java.lang.reflect.Type
import com.google.gson.{
  JsonSerializer,JsonSerializationContext,JsonElement,JsonPrimitive
}

import de.unikl.cs.dbis.waves.util.nested.schemas._

/**
  * Sort the rows lexicographically by their definition level where columns are
  * considered in ascending order by the number of their distinct values
  * 
  * See: Lemire and Kaser, Reordering columns for smaller indexes, 2011 
  */
object LexicographicSorter extends Sorter {

  override val name = "lexicographic"

  private val SORT_GROUPER = DefinitionLevelGrouper

  override def grouper: Grouper = SORT_GROUPER
  override def sort(bucket: DataFrame): DataFrame = {
    val leafs = leafCount(bucket)
    val counts = Range(0, leafs).map(i => count_distinct(indexedColumn(i)))
    val aggregate = bucket.agg(counts.head, counts.tail:_*).head()
    val numberOfLevels = Range(0, leafs).map(i => aggregate.getLong(i))
    val order = numberOfLevels.zipWithIndex
                              .sortBy(_._1)
                              .map{case (_, i) => indexedColumn(i)}
    bucket.orderBy(order:_*)
  }

  private def leafCount(bucket: DataFrame): Int = {
    val colname = "size"
    val row = bucket.select(size(SORT_GROUPER.GROUP_COLUMN).as(colname)).head()
    val index = row.fieldIndex(colname)
    row.getInt(index)
  }

  private def indexedColumn(i: Int): Column = SORT_GROUPER.GROUP_COLUMN(i)
}

object LexicographicSorterSerializer extends JsonSerializer[LexicographicSorter.type] {
  override def serialize(src: LexicographicSorter.type, typeOfSrc: Type, ctx: JsonSerializationContext): JsonElement
    = new JsonPrimitive(LexicographicSorter.name)
}
