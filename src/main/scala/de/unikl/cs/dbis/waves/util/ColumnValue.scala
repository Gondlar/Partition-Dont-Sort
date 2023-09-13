package de.unikl.cs.dbis.waves.util

import collection.JavaConverters._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.typedlit
import org.apache.spark.sql.types._

import com.google.gson.JsonObject

sealed trait ColumnValue {

  /**
    * @param rhs
    * @return true iff rhs represents the same type as this
    */
  def isOfSameTypeAs(rhs: ColumnValue): Boolean

  /**
    * @return This value represented as a JSON Object for Gson
    */
  def toGson: JsonObject

  /**
    * @return This value represented as a Spark Literal
    */
  def toLiteral: Column

  /**
    * @param rhs
    * @return true iff this value is less than or equal to rhs' value. rhs must
    *         be of the same type
    */
  def <=(rhs: ColumnValue): Boolean

  /**
    * @param rhs
    * @return true iff this value is strictly less than rhs' value. rhs must be
    *         of the same type
    */
  def <(rhs: ColumnValue): Boolean

  /**
    * @param rhs
    * @return true iff this value is greater than or equal to rhs' value. rhs
    *         must be of the same type
    */
  def >=(rhs: ColumnValue): Boolean = rhs <= this

  /**
    * @param rhs
    * @return true iff this value is strictly greater than rhs' value. rhs must
    *         be of the same type
    */
  def >(rhs: ColumnValue): Boolean = rhs < this

  /**
    * Find the value that is at the given percentage between this value and the
    * given one. The other value must represent the same type.
    *
    * @param rhs the other value
    * @param distance the percentage of the way between this and rhs
    * @return the interpolated value
    */
  def interpolate(rhs: ColumnValue, distance: Double): ColumnValue

  /**
    * @return the successor of this value or this value again if there is no
    *         clear successor. For example, the successor(5) yield 6, but
    *         successor(true) is true again.
    */
  def successor: ColumnValue
}

object ColumnValue {
  implicit def fromBoolean(value: Boolean) = BooleanColumn(value)
  implicit def fromInt(value: Int) = IntegerColumn(value)
  implicit def fromLong(value: Long) = LongColumn(value)
  implicit def fromDouble(value: Double) = DoubleColumn(value)
  implicit def fromString(value: String) = StringColumn(value)

  def fromAny(value: Any): Option[ColumnValue] = value match {
    case v: Boolean => Some(BooleanColumn(v))
    case v: Int => Some(IntegerColumn(v))
    case v: Long => Some(LongColumn(v))
    case v: Double => Some(DoubleColumn(v))
    case v: String => Some(StringColumn(v))
    case _ => None
  }

  def fromRow(row: Row, schema: StructType, index: Int): Option[ColumnValue]
    = schema.fields(index).dataType match {
      case BooleanType => Some(BooleanColumn(row.getBoolean(index)))
      case IntegerType => Some(IntegerColumn(row.getInt(index)))
      case LongType => Some(LongColumn(row.getLong(index)))
      case DoubleType => Some(DoubleColumn(row.getDouble(index)))
      case StringType => Some(StringColumn(row.getString(index)))
      case _ => None
    }

  def fromRow(row: Row, index: Int): Option[ColumnValue]
    = fromRow(row, row.schema, index)

  val TYPE_IDENTIFIER_KEY = "type"
  val TYPE_IDENTIFIER_BOOLEAN = "bool"
  val TYPE_IDENTIFIER_INT = "int"
  val TYPE_IDENTIFIER_LONG = "long"
  val TYPE_IDENTIFIER_DOUBLE = "double"
  val TYPE_IDENTIFIER_STRING = "string"
  val VALUE_KEY = "value"

  def fromGson(json: JsonObject): Option[ColumnValue] = {
    if (!json.has(TYPE_IDENTIFIER_KEY) || !json.has(VALUE_KEY))
      return None
    val value = json.get(VALUE_KEY)
    json.get(TYPE_IDENTIFIER_KEY).getAsString() match {
      case TYPE_IDENTIFIER_BOOLEAN => Some(value.getAsBoolean())
      case TYPE_IDENTIFIER_INT => Some(value.getAsInt())
      case TYPE_IDENTIFIER_LONG => Some(value.getAsLong())
      case TYPE_IDENTIFIER_DOUBLE => Some(value.getAsDouble())
      case TYPE_IDENTIFIER_STRING => Some(value.getAsString())
      case _ => None
    }
  }
}

final case class BooleanColumn(v: Boolean) extends ColumnValue {

  override def isOfSameTypeAs(rhs: ColumnValue): Boolean
    = rhs.isInstanceOf[BooleanColumn]

  override def toGson: JsonObject = {
    val obj = new JsonObject()
    obj.addProperty(ColumnValue.TYPE_IDENTIFIER_KEY, ColumnValue.TYPE_IDENTIFIER_BOOLEAN)
    obj.addProperty(ColumnValue.VALUE_KEY, v)
    obj
  }

  override def toLiteral: Column = typedlit(v)

  override def <=(rhs: ColumnValue): Boolean = {
    assert(isOfSameTypeAs(rhs))
    v <= rhs.asInstanceOf[BooleanColumn].v
  }

  override def <(rhs: ColumnValue): Boolean = {
    assert(isOfSameTypeAs(rhs))
    v < rhs.asInstanceOf[BooleanColumn].v
  }

  override def interpolate(rhs: ColumnValue, distance: Double): ColumnValue = {
    assert(isOfSameTypeAs(rhs))
    if (rhs.asInstanceOf[BooleanColumn].v == v) this else BooleanColumn(false)
  }

  override def successor: ColumnValue = BooleanColumn(true)
}

final case class IntegerColumn(v: Int) extends ColumnValue {

  override def isOfSameTypeAs(rhs: ColumnValue): Boolean
    = rhs.isInstanceOf[IntegerColumn]

  override def toGson: JsonObject = {
    val obj = new JsonObject()
    obj.addProperty(ColumnValue.TYPE_IDENTIFIER_KEY, ColumnValue.TYPE_IDENTIFIER_INT)
    obj.addProperty(ColumnValue.VALUE_KEY, v)
    obj
  }

  override def toLiteral: Column = typedlit(v)

  override def <=(rhs: ColumnValue): Boolean = {
    assert(isOfSameTypeAs(rhs))
    v <= rhs.asInstanceOf[IntegerColumn].v
  }

  override def <(rhs: ColumnValue): Boolean = {
    assert(isOfSameTypeAs(rhs))
    v < rhs.asInstanceOf[IntegerColumn].v
  }

  override def interpolate(rhs: ColumnValue, distance: Double): IntegerColumn = {
    assert(isOfSameTypeAs(rhs))
    val partialRange = ((rhs.asInstanceOf[IntegerColumn].v - v + 1) * distance).toInt
    IntegerColumn(v + partialRange - 1)
  }

  override def successor: IntegerColumn = IntegerColumn(v+1)
}

final case class LongColumn(v: Long) extends ColumnValue {

  override def isOfSameTypeAs(rhs: ColumnValue): Boolean
    = rhs.isInstanceOf[LongColumn]

  override def toGson: JsonObject = {
    val obj = new JsonObject()
    obj.addProperty(ColumnValue.TYPE_IDENTIFIER_KEY, ColumnValue.TYPE_IDENTIFIER_LONG)
    obj.addProperty(ColumnValue.VALUE_KEY, v)
    obj
  }

  override def toLiteral: Column = typedlit(v)

  override def <=(rhs: ColumnValue): Boolean = {
    assert(isOfSameTypeAs(rhs))
    v <= rhs.asInstanceOf[LongColumn].v
  }

  override def <(rhs: ColumnValue): Boolean = {
    assert(isOfSameTypeAs(rhs))
    v < rhs.asInstanceOf[LongColumn].v
  }

  override def interpolate(rhs: ColumnValue, distance: Double): LongColumn = {
    val partialRange = ((rhs.asInstanceOf[LongColumn].v - v + 1) * distance).toLong
    LongColumn(v + partialRange - 1)
  }

  override def successor: LongColumn = LongColumn(v+1)
}

final case class DoubleColumn(v: Double) extends ColumnValue {

  override def isOfSameTypeAs(rhs: ColumnValue): Boolean
  = rhs.isInstanceOf[DoubleColumn]

  override def toGson: JsonObject = {
    val obj = new JsonObject()
    obj.addProperty(ColumnValue.TYPE_IDENTIFIER_KEY, ColumnValue.TYPE_IDENTIFIER_DOUBLE)
    obj.addProperty(ColumnValue.VALUE_KEY, v)
    obj
  }

  override def toLiteral: Column = typedlit(v)

  override def <=(rhs: ColumnValue): Boolean = {
    assert(isOfSameTypeAs(rhs))
    v <= rhs.asInstanceOf[DoubleColumn].v
  }

  override def <(rhs: ColumnValue): Boolean = {
    assert(isOfSameTypeAs(rhs))
    v < rhs.asInstanceOf[DoubleColumn].v
  }

  override def interpolate(rhs: ColumnValue, distance: Double): DoubleColumn = {
    assert(isOfSameTypeAs(rhs))
    val partialRange = (rhs.asInstanceOf[DoubleColumn].v - v) * distance
    DoubleColumn(v + partialRange)
  }

  override def successor: DoubleColumn = this
}

final case class StringColumn(v: String) extends ColumnValue {

  override def isOfSameTypeAs(rhs: ColumnValue): Boolean
    = rhs.isInstanceOf[StringColumn]

  override def toGson: JsonObject = {
    val obj = new JsonObject()
    obj.addProperty(ColumnValue.TYPE_IDENTIFIER_KEY, ColumnValue.TYPE_IDENTIFIER_STRING)
    obj.addProperty(ColumnValue.VALUE_KEY, v)
    obj
  }

  override def toLiteral: Column = typedlit(v)

  override def <=(rhs: ColumnValue): Boolean = {
    assert(isOfSameTypeAs(rhs))
    v <= rhs.asInstanceOf[StringColumn].v
  }

  override def <(rhs: ColumnValue): Boolean = {
    assert(isOfSameTypeAs(rhs))
    v < rhs.asInstanceOf[StringColumn].v
  }

  override def interpolate(rhs: ColumnValue, distance: Double): StringColumn = {
    assert(isOfSameTypeAs(rhs))

    val (paddedMin, paddedMax) = zeroPaddedStrings(v, rhs.asInstanceOf[StringColumn].v)
    var carry: Char = 0
    val mid = for {
      (minChar, maxChar) <- paddedMin.zip(paddedMax).reverse
    } yield {
      val adjustedMinChar = minChar + carry
      val toNextRollover = Char.MaxValue - adjustedMinChar
      val range = if (maxChar >= adjustedMinChar) maxChar-adjustedMinChar else maxChar+toNextRollover
      val partialDistance = (range*distance).toChar
      carry = if (partialDistance > toNextRollover) 1 else 0
      (adjustedMinChar+partialDistance).toChar
    }
    StringColumn(new String(mid.reverse.toArray))
  }

  /**
    * @return (min, max) such that the shorter of them (if any) is padded with
    *         one \0 character at the end and the longer string is truncated at
    *         that length. This ensures that all strings are at least of length
    *         1.
    */
  private def zeroPaddedStrings(min: String, max: String) = {
    val minLen = min.length()
    val maxLen = max.length()
    if (minLen < maxLen) (s"$min\0", max.substring(0, minLen+1))
    else if (maxLen < minLen) (min.substring(0, maxLen+1), s"$max\0")
    else (min, max)
  }

  override def successor: StringColumn
    = StringColumn(s"${v.substring(0, v.size-1)}${(v.last+1).toChar}")
}
