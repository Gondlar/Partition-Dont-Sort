package de.unikl.cs.dbis.waves.parquet

import org.apache.parquet.filter2.predicate.Operators.{ LongColumn
                                                      , DoubleColumn
                                                      , BinaryColumn
                                                      , BooleanColumn
                                                      , IntColumn
                                                      , FloatColumn
                                                      , Column
                                                      , SupportsEqNotEq
}
import org.apache.parquet.filter2.predicate.{FilterApi,FilterPredicate}
import org.apache.parquet.io.api.Binary

import org.apache.spark.sql.types.StructType

import de.unikl.cs.dbis.waves.util.PathKey
import org.apache.spark.sql.types.DataType

sealed trait EqNeqColumn[T] {
    type ParquetType <: Comparable[ParquetType]
    type ColumnType <: Column[ParquetType] with SupportsEqNotEq

    def convert(value : Any) : ParquetType = value.asInstanceOf[ParquetType]
    def makeColumn(name : String) : ColumnType
}

object EqNeqColumn {
    implicit val intColumnTypes = new EqNeqColumn[Int] {
        override type ParquetType = java.lang.Integer
        override type ColumnType = IntColumn

        override def makeColumn(name: String): ColumnType = FilterApi.intColumn(name)
    }

    implicit val longColumnTypes = new EqNeqColumn[Long] {
        override type ParquetType = java.lang.Long
        override type ColumnType = LongColumn

        override def makeColumn(name: String): ColumnType = FilterApi.longColumn(name)
    }

    implicit val floatColumnTypes = new EqNeqColumn[Float] {
        override type ParquetType = java.lang.Float
        override type ColumnType = FloatColumn

        override def makeColumn(name: String): ColumnType = FilterApi.floatColumn(name)
    }

    implicit val doubleColumnTypes = new EqNeqColumn[Double] {
        override type ParquetType = java.lang.Double
        override type ColumnType = DoubleColumn

        override def makeColumn(name: String): ColumnType = FilterApi.doubleColumn(name)
    }

    implicit val stringColumnTypes = new EqNeqColumn[String] {
        override type ParquetType = Binary
        override type ColumnType = BinaryColumn

        override def convert(value: Any): ParquetType
            = if (value == null) null else Binary.fromString(value.asInstanceOf[String])
        override def makeColumn(name: String): ColumnType = FilterApi.binaryColumn(name)
    }

    implicit val booleanColumnTypes = new EqNeqColumn[Boolean] {
        override type ParquetType = java.lang.Boolean
        override type ColumnType = BooleanColumn

        override def makeColumn(name: String): ColumnType = FilterApi.booleanColumn(name)
    }

    def filter(name : String, value : Any, schema : StructType, f : EqNeqFilter) : Option[FilterPredicate]
        = PathKey(name).retrieveFrom(schema).flatMap(tpe => filter(name, value, tpe, f))

    def filter(name : String, value : Any, tpe : DataType, filter : EqNeqFilter) : Option[FilterPredicate]
        = tpe.typeName match {
            case "string" => Some(filter[String](name, value))
            case "int" => Some(filter[Int](name, value)) //TODO currect type string?
            case "long" => Some(filter[Long](name, value))
            case "float" => Some(filter[Float](name, value))
            case "double" => Some(filter[Double](name, value))
            case "boolean" => Some(filter[Boolean](name, value))
            case _ => None
        }
}

sealed trait EqNeqFilter {
    def apply[T](name : String, value : Any)(implicit ct : EqNeqColumn[T]) : FilterPredicate
}

object EqNeqFilter {
    val eqFilter = new EqNeqFilter {
        override def apply[T](name : String, value : Any)(implicit ct : EqNeqColumn[T]) : FilterPredicate
            = FilterApi.eq[ct.ParquetType,ct.ColumnType](ct.makeColumn(name), ct.convert(value))
    }

    val notEqFilter = new EqNeqFilter {
        override def apply[T](name : String, value : Any)(implicit ct : EqNeqColumn[T]) : FilterPredicate
            = FilterApi.notEq[ct.ParquetType,ct.ColumnType](ct.makeColumn(name), ct.convert(value))
    }

    val eqOrNullFilter = new EqNeqFilter {
        override def apply[T](name : String, value : Any)(implicit ct : EqNeqColumn[T]) : FilterPredicate = {
            val col = ct.makeColumn(name)
            val eq = FilterApi.eq[ct.ParquetType,ct.ColumnType](col, ct.convert(value))
            val eqNull = FilterApi.eq[ct.ParquetType,ct.ColumnType](col, null.asInstanceOf[ct.ParquetType])
            FilterApi.or(eq, eqNull)
        }
    }
}
