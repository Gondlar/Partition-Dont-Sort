package de.unikl.cs.dbis.waves.split

import de.unikl.cs.dbis.waves.util.operators.{Grouper, DefinitionLevelGrouper}
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.count_distinct

import de.unikl.cs.dbis.waves.util.nested.schemas._

/**
  * Sort the rows lexicographically by their definition level where columns are
  * considered in ascending order by the number of their distinct values
  * 
  * See: Lemire and Kaser, Reordering columns for smaller indexes, 2011 
  */
trait LexicographicSorter extends GroupedSplitter {

  private val SORT_GROUPER = DefinitionLevelGrouper

  override protected def sortGrouper: Grouper = SORT_GROUPER
  override protected def sort(bucket: DataFrame): DataFrame = {
    val leafs = data.schema.optionalLeafCount()
    val counts = Range(0, leafs).map(i => count_distinct(indexedColumn(i)))
    val aggregate = bucket.agg(counts.head, counts.tail:_*).head()
    val numberOfLevels = Range(0, leafs).map(i => aggregate.getLong(i))
    val order = numberOfLevels.zipWithIndex
                              .sortBy(_._1)
                              .map{case (_, i) => indexedColumn(i)}
    bucket.orderBy(order:_*)
  }

  private def indexedColumn(i: Int): Column = SORT_GROUPER.GROUP_COLUMN(i)
}
