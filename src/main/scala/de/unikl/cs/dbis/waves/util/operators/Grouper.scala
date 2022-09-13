package de.unikl.cs.dbis.waves.util.operators

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{
  typedLit, collect_list, map_from_entries, struct, col, monotonically_increasing_id, sum
}
import org.apache.spark.sql.types.StructType

abstract class Grouper(
  val GROUP_COLUMN: TempColumn,
) {

  private val INDEX_COLUMN = TempColumn("index")
  private val MAP_COLUMN = TempColumn("map")
  private val STRUCT_COLUMN = TempColumn("structs")
  private val ORDER_COLUMN = TempColumn("order")
  private val SORT_COLUMN = TempColumn("sort")
  val PARTITION_COLUMN = TempColumn("partition")
  val COUNT_COLUMN = col("count")

  def apply(schema: StructType): Column

  /**
    * Alias for [[group]]
    */
  def apply(data: DataFrame): DataFrame = group(data)

  /**
    * Given a data frame, count how many rows of each kind it contains.
    *
    * @param data the data to group
    * @return the grouped data. It consists of two columns. One stores the kind
    *         of the data and is named as given by GROUP_COLUMN. The other gives
    *         the count and is named as given in COUNT_COLUMN.
    */
  def group(data: DataFrame): DataFrame
    = data.groupBy(this(data.schema)).count()

  /**
    * Given a sequence of disjoint DataFrames containing the grouped
    * representation and a DataFrame containing the origional data, return a
    * DataFrame which has an additional column with the position of the index of
    * the group it belonges to. That column's name can be found in
    * PARTITION_COLUMN.
    *
    * @param buckets the grouped represenatations
    * @param data the actual data. It must have the same schema as used to 
    *             create the grouped entries in the buckets.
    * @return data with an additional column.
    */
  def matchAll(buckets: Seq[DataFrame], data: DataFrame) = {
    val map = dataFrameToMap(unionWithIndex(buckets), GROUP_COLUMN, INDEX_COLUMN)
    data.crossJoin(map)
        .withColumn(PARTITION_COLUMN, MAP_COLUMN(this(data.schema)))
        .drop(map.columns:_*)
  }

  /**
    * Sort the given data frame such that it is orderd in the same order as the
    * given grouped data frame.
    *
    * @param bucket the "sort order"
    * @param data The data to sort. It must have the same schema as used to
    *             create the grouped entries in bucket.
    * @return data, but sorted
    */
  def sort(bucket: DataFrame, data: DataFrame) = {
    val map = dataFrameToMap(withOrderedId(bucket), GROUP_COLUMN, ORDER_COLUMN)
    data.crossJoin(map)
        // Determine sort order
        .withColumn(SORT_COLUMN, MAP_COLUMN(this(data.schema)))
        // Sort
        .repartition(1)
        .sort(SORT_COLUMN.col)
        // Remove all intermediary data
        .drop(SORT_COLUMN.col)
        .drop(map.columns:_*)
  }

  /**
    * Given a data frame which follows the groupers grouped schema, determine
    * how many original data rows the contained groups represent
    *
    * @param df the data frame
    * @return the count
    */
  def count(df: DataFrame) = df.agg(sum(COUNT_COLUMN)).head.getLong(0)

  /**
    * Given a DataFrame and two of its Columns, agggregate the data frame such
    * that it contains one row which is a map between the given columns. Both
    * columns must be deterministic. The resulting map resides in a column whose
    * name can be found in MAP_COLUMN.
    *
    * @param df the data frame to aggregate
    * @param key the column which contains the keys
    * @param value the column which contains the values
    * @return the aggregated data frame
    */
  protected def dataFrameToMap(df: DataFrame, key: Column, value: Column) = {
    assert(key.expr.deterministic)
    assert(value.expr.deterministic)
    df.agg(collect_list(struct(key, value)).as(STRUCT_COLUMN))
      .select(map_from_entries(STRUCT_COLUMN).as(MAP_COLUMN))
  }

  /**
    * Given a sequence of data disjoint data frames, return a data frame which
    * if the union of all given ones with an additional column containing the
    * index of the data frame in the sequence where it originated from.
    * The name of the index column can be found in INDEX_COLUMN
    *
    * @param buckets the data
    * @return the union of tha data
    */
  private def unionWithIndex(buckets: Seq[DataFrame]): DataFrame = {
    buckets.zipWithIndex.map { case (bucket, index) =>
      bucket.select(GROUP_COLUMN.bind(bucket), typedLit(index).as(INDEX_COLUMN))
    }.reduce{(lhs, rhs) =>
      lhs.unionAll(rhs)
    }
  }

  /**
    * Add a monotonoically increasing id to the given (sorted) data frame. The
    * new column's name can be found in ORDER_COLUMN.
    *
    * @param df the data
    * @return the data with the additional column
    */
  private def withOrderedId(df: DataFrame)
    = df.withColumn(ORDER_COLUMN, monotonically_increasing_id)
}
