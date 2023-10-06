package de.unikl.cs.dbis.waves.util

import org.apache.spark.internal.Logging
import scala.collection.mutable.WrappedArray

/**
  * Metadata representing (assumed) unniformly distributed values in a column
  *
  * @param min the minimum value
  * @param max the maximum value
  * @param distinct the number of distinct values
  */
final case class UniformColumnMetadata(
  val min: ColumnValue,
  val max: ColumnValue,
  override val distinct: Long
) extends ColumnMetadata {
  assert(distinct >= 1)
  assert(min isOfSameTypeAs max)
  assert(min <= max)

  
  override def gini = (distinct-1)/distinct.toDouble

  
  override def separator(quantile: Double = .5): ColumnValue
    = min.interpolate(max, quantile)

  override def split(quantile: Double = .5): Either[String,(UniformColumnMetadata, UniformColumnMetadata)] = {
    if (distinct == 1 || min >= max) Left("range cannot be split") else {
      val lowBoundary = separator(quantile)
      val highBoundary = lowBoundary.successor
      val lessValues = (distinct*quantile).toLong
      val greaterValues = distinct - lessValues
      Right((
        UniformColumnMetadata(min, lowBoundary, lessValues),
        UniformColumnMetadata(highBoundary, max, greaterValues)
      ))
    }
  }
}

object UniformColumnMetadata extends Logging {
  def fromSeq(data: Seq[Any], minIndex: Int = 0, maxIndex: Int = 1, distinctIndex: Int = 2): Option[ColumnMetadata] = {
    val distinct = data(distinctIndex).asInstanceOf[Long]
    if (distinct == 0) None else try {
      val col = for {
        min <- ColumnValue.fromAny(data(minIndex))
        max <- ColumnValue.fromAny(data(maxIndex))
      } yield UniformColumnMetadata(min, max, distinct)
      if (col.isEmpty && !data(minIndex).isInstanceOf[WrappedArray[_]])
        logWarning(s"Ignoring Colum $minIndex with unimplemented type ${data(minIndex).getClass().getName()}")
      col
    } catch {
      case e: IllegalArgumentException => None
    }
  }
}

