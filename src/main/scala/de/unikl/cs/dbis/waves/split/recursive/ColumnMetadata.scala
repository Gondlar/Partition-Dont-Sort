package de.unikl.cs.dbis.waves.split.recursive

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import collection.JavaConverters._
import org.apache.spark.sql.types.StringType

sealed trait ColumnMetadata[+Type] {
  protected def min: Type
  protected def max: Type
  def distinct: Long

  assert(distinct >= 1)

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
  def separator(quantile: Double = .5): Type
  def split(quantile: Double = .5): Either[String,(ColumnMetadata[Type], ColumnMetadata[Type])]
}

object ColumnMetadata extends Logging {
  def fromRows(data: Row, minIndex: Int, maxIndex: Int, distinctIndex: Int): Option[ColumnMetadata[_]] = {
    val distinct = data.getLong(distinctIndex)
    if (distinct == 0) None else try {
      val tpe = data.schema.fields(minIndex).dataType
      assert(data.schema.fields(maxIndex).dataType == tpe)
      tpe match {
        case BooleanType => Some(BooleanColumnMetadata(distinct))
        case IntegerType => Some(IntColumnMetadata(data.getInt(minIndex), data.getInt(maxIndex), distinct))
        case LongType => Some(LongColumnMetadata(data.getLong(minIndex), data.getLong(maxIndex), distinct))
        case DoubleType => Some(DoubleColumnMetadata(data.getDouble(minIndex), data.getDouble(maxIndex), distinct))
        case StringType => Some(StringColumnMetadata(data.getString(minIndex), data.getString(maxIndex), distinct))
        case ArrayType(_, _) => None // silently ignore array
        case _ => {
          logWarning(s"Ignoring Colum $minIndex with unimplemented type $tpe")
          None
        }
      }
    } catch {
      case e: IllegalArgumentException => None
    }
  }
}

final case class BooleanColumnMetadata(
  distinct: Long
) extends ColumnMetadata[Boolean] {
  assert(distinct == 1 || distinct == 2)

  override def min: Boolean = false
  override def max: Boolean = true

  override def separator(quantile: Double): Boolean = false

  override def split(quantile: Double): Either[String,(BooleanColumnMetadata, BooleanColumnMetadata)] = {
    if (distinct == 1) Left("range cannot be split") else {
      Right((BooleanColumnMetadata(1), BooleanColumnMetadata(1)))
    }
  }
}

final case class IntColumnMetadata(
  min: Int,
  max: Int,
  distinct: Long
) extends ColumnMetadata[Int] {
  assert(min <= max, s"min: $min <= max: $max")

  override def separator(quantile: Double): Int = {
    val partialRange = ((max - min + 1)*quantile).toInt
    min + partialRange - 1
  }
  override def split(quantile: Double): Either[String,(IntColumnMetadata, IntColumnMetadata)] = {
    if (distinct == 1 || min == max) Left("range cannot be split") else {
      val partialValues = (distinct*quantile).toLong
      val boundary = separator(quantile)
      Right((IntColumnMetadata(min, boundary, partialValues), IntColumnMetadata(boundary + 1, max, distinct-partialValues)))
    }
  }
}

final case class LongColumnMetadata(
  min: Long,
  max: Long,
  distinct: Long
) extends ColumnMetadata[Long] {
  assert(min <= max, s"min: $min <= max: $max")

  override def separator(quantile: Double): Long = {
    val partialRange = ((max - min + 1)*quantile).toLong
    min + partialRange - 1
  }

  override def split(quantile: Double): Either[String,(LongColumnMetadata, LongColumnMetadata)] = {
    if (distinct == 1 || min == max) Left("range cannot be split") else {
      val partialValues = (distinct*quantile).toLong
      val boundary = separator(quantile)
      Right(LongColumnMetadata(min, boundary, partialValues), LongColumnMetadata(boundary + 1, max, distinct-partialValues))
    }
  }
}

final case class DoubleColumnMetadata(
  min: Double,
  max: Double,
  distinct: Long
) extends ColumnMetadata[Double] {
  assert(min <= max, s"min: $min <= max: $max")

  override def separator(quantile: Double): Double = {
    val partialRange = (max - min)*quantile
    min + partialRange
  }
  override def split(quantile: Double): Either[String,(DoubleColumnMetadata, DoubleColumnMetadata)] = {
    if (distinct == 1 || min == max) Left("range cannot be split") else {
      val partialValues = (distinct*quantile).toLong
      val boundary = separator(quantile)
      Right(DoubleColumnMetadata(min, boundary, partialValues), DoubleColumnMetadata(boundary, max, distinct-partialValues))
    }
  }
}

final case class StringColumnMetadata(
  min: String,
  max: String,
  distinct: Long
) extends ColumnMetadata[String] {
  assert(min <= max, s"min: $min <= max: $max")

  override def separator(quantile: Double): String = {
    val minBytes = min.codePoints().iterator().asScala
    val maxBytes = max.codePoints().iterator().asScala
    val middle = for {
      (minCodePoint, maxCodePoint) <- minBytes.zip(maxBytes)
      middle = ((maxCodePoint - minCodePoint)*quantile).toInt + minCodePoint // this will not work
      ch <- Character.toChars(middle)
    } yield ch
    new String(middle.toArray)
  }

  override def split(quantile: Double): Either[String,(StringColumnMetadata, StringColumnMetadata)] = {
    if (distinct == 1 || min == max) Left("range cannot be split") else {
      val partialValues = (distinct*quantile).toLong
      val boundary = separator(quantile)
      val next = s"${boundary.substring(0, boundary.size-1)}${(boundary.last+1).toChar}"
      Right(StringColumnMetadata(min, boundary, partialValues), StringColumnMetadata(next, max, distinct-partialValues))
    }
  }


}
