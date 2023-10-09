package de.unikl.cs.dbis.waves.util

final case class BooleanColumnMetadata(
  truePercentage: Double
) extends ColumnMetadata {
  import BooleanColumnMetadata._

  assert(0 <= truePercentage && truePercentage <= 1)

  override def distinct: Long
    = if (truePercentage == 0 || truePercentage == 1) 1 else 2

  override def gini: Double = {
    val falsePercentage = (1-truePercentage)
    2 * truePercentage * falsePercentage // == 1 - truePercentage² - falsePercentage²
  }

  override def separator(quantile: Double): ColumnValue = false

  override def split(quantile: Double): Either[String,(BooleanColumnMetadata, BooleanColumnMetadata)]
    = Either.cond(distinct == 2, (allFalse, allTrue), "range cannot be split")
}

object BooleanColumnMetadata {
  val allTrue = BooleanColumnMetadata(1)
  val allFalse = BooleanColumnMetadata(0)
  val uniform = BooleanColumnMetadata(.5)

  def fromCounts(falseCount: Long, trueCount: Long): Option[BooleanColumnMetadata] = {
    assert(falseCount >= 0)
    assert(trueCount >= 0)
    val total = falseCount + trueCount
    if (total > 0) Some(BooleanColumnMetadata(trueCount/total.toDouble)) else None
  }
}
