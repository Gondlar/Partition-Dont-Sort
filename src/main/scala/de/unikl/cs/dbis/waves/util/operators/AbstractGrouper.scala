package de.unikl.cs.dbis.waves.util.operators

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{
  typedLit, collect_list, map_from_entries, struct, col, monotonically_increasing_id, sum
}
import org.apache.spark.sql.types.StructType

/**
  * The AbstractGrouper assumes a group representation which consists of a
  * (optionally nested) Column containing the information identifying the group
  * and another column storing the number of source Rows represented by that
  * group
  *
  * @param GROUP_COLUMN the name of the Column containing the group information
  */
abstract class AbstractGrouper(
  val GROUP_COLUMN: TempColumn,
) extends Grouper {

  private val INDEX_COLUMN = TempColumn("index")
  private val MAP_COLUMN = TempColumn("map")
  private val STRUCT_COLUMN = TempColumn("structs")
  private val ORDER_COLUMN = TempColumn("order")
  private val SORT_COLUMN = TempColumn("sort")
  val MATCH_COLUMN = TempColumn("partition")
  val COUNT_COLUMN = col("count")

  def apply(schema: StructType): Column

  /**
    * @return the columns' names in the this grouper's grouped representation
    */
  def columns: Seq[String] = Seq(GROUP_COLUMN, COUNT_COLUMN.toString())

  /**
    * Given a source data frame, count how many rows of each group it contains.
    *
    * @param data the data to group
    * @return the grouped data. It consists of two columns. One stores the kind
    *         of the data and is named as given by GROUP_COLUMN. The other gives
    *         the count and is named as given in COUNT_COLUMN.
    */
  override def group(data: DataFrame): DataFrame
    = data.groupBy(this(data.schema)).count()

  override def find(bucket: DataFrame, data: DataFrame) = {
    data.join(bucket, GROUP_COLUMN.bind(bucket) === this(data.schema), "leftsemi")
  }

  override def matchAll(buckets: Seq[DataFrame], data: DataFrame) = {
    val map = dataFrameToMap(unionWithIndex(buckets), GROUP_COLUMN, INDEX_COLUMN)
    data.crossJoin(map)
        .withColumn(MATCH_COLUMN, MAP_COLUMN(this(data.schema)))
        .drop(map.columns:_*)
  }

  override def matchColumn: TempColumn = MATCH_COLUMN

  override def sort(bucket: DataFrame, data: DataFrame) = {
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

  override def count(df: DataFrame) = df.agg(sum(COUNT_COLUMN)).head.getLong(0)

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
    * Given a sequence of disjoint data frames, return a data frame which
    * is the union of all given ones with an additional column containing the
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
