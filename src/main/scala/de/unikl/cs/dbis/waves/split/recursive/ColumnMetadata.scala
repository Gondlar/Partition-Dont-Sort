package de.unikl.cs.dbis.waves.split.recursive

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType

import de.unikl.cs.dbis.waves.util.ColumnValue

final case class ColumnMetadata(
  val min: ColumnValue,
  val max: ColumnValue,
  val distinct: Long
) {
  assert(distinct >= 1)
  assert(min isOfSameTypeAs max)
  assert(min <= max)

  /**
    * @return The GiniCoefficient of the data in this column
    */
  def gini = (distinct-1)/distinct.toDouble

  /**
    * The separator is the highest value that would go into the left bucket
    * after a split
    *
    * @param quantile the percantage of values to go in the left bucket
    * @return the separator
    */
  def separator(quantile: Double = .5): ColumnValue
    = min.interpolate(max, quantile)

  def split(quantile: Double = .5): Either[String,(ColumnMetadata, ColumnMetadata)] = {
    if (distinct == 1 || min >= max) Left("range cannot be split") else {
      val lowBoundary = separator(quantile)
      val highBoundary = lowBoundary.successor
      val lessValues = (distinct*quantile).toLong
      val greaterValues = distinct - lessValues
      Right((
        ColumnMetadata(min, lowBoundary, lessValues),
        ColumnMetadata(highBoundary, max, greaterValues)
      ))
    }
  }
}

object ColumnMetadata extends Logging {
  def fromRows(data: Row, minIndex: Int, maxIndex: Int, distinctIndex: Int): Option[ColumnMetadata] = {
    val distinct = data.getLong(distinctIndex)
    if (distinct == 0) None else try {
      val col = for {
        min <- ColumnValue.fromRow(data, minIndex)
        max <- ColumnValue.fromRow(data, maxIndex)
      } yield ColumnMetadata(min, max, data.getLong(distinctIndex))
      if (col.isEmpty && !data.schema.fields(minIndex).dataType.isInstanceOf[ArrayType])
        logWarning(s"Ignoring Colum $minIndex with unimplemented type ${data.schema.fields(minIndex).dataType}")
      col
    } catch {
      case e: IllegalArgumentException => None
    }
  }
}
