package de.unikl.cs.dbis.waves.util.operators

import org.apache.spark.sql.{functions => sf}
import org.apache.spark.sql.DataFrame
import java.util.UUID

final class TempColumn(name: String) {
  def col() = sf.col(name)
  def bind(df: DataFrame) = df.col(name)
  override def toString(): String = name
}

object TempColumn {
  implicit def toColumn(col: TempColumn) = col.col()
  implicit def toName(col: TempColumn) = col.toString()

  def apply(name: String) = new TempColumn(s"$name-${UUID.randomUUID}")
}
