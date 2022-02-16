package de.unikl.cs.dbis.waves.parquet

import org.apache.parquet.filter2.predicate.Operators.{ LongColumn
                                                      , DoubleColumn
                                                      , BinaryColumn
                                                      , BooleanColumn
                                                      , IntColumn
                                                      , FloatColumn
                                                      , Column
                                                      , SupportsLtGt
}
import org.apache.parquet.filter2.predicate.{FilterApi,FilterPredicate}
import org.apache.parquet.io.api.Binary

import org.apache.spark.sql.types.StructType

import de.unikl.cs.dbis.waves.util.PathKey
import org.apache.parquet.filter2.predicate.UserDefinedPredicate
import org.apache.parquet.filter2.predicate.Statistics
import org.apache.parquet.schema.PrimitiveComparator

sealed trait LtGtColumn[T] {
    type ParquetType <: Comparable[ParquetType]
    type ColumnType <: Column[ParquetType] with SupportsLtGt

    def convert(value : Any) : ParquetType = value.asInstanceOf[ParquetType]
    def makeColumn(name : String) : ColumnType
}

object LtGtColumn {
    implicit val intColumnTypes = new LtGtColumn[Int] {
        override type ParquetType = java.lang.Integer
        override type ColumnType = IntColumn

        override def makeColumn(name: String): ColumnType = FilterApi.intColumn(name)
    }

    implicit val longColumnTypes = new LtGtColumn[Long] {
        override type ParquetType = java.lang.Long
        override type ColumnType = LongColumn

        override def makeColumn(name: String): ColumnType = FilterApi.longColumn(name)
    }

    implicit val floatColumnTypes = new LtGtColumn[Float] {
        override type ParquetType = java.lang.Float
        override type ColumnType = FloatColumn

        override def makeColumn(name: String): ColumnType = FilterApi.floatColumn(name)
    }

    implicit val doubleColumnTypes = new LtGtColumn[Double] {
        override type ParquetType = java.lang.Double
        override type ColumnType = DoubleColumn

        override def makeColumn(name: String): ColumnType = FilterApi.doubleColumn(name)
    }

    implicit val stringColumnTypes = new LtGtColumn[String] {
        override type ParquetType = Binary
        override type ColumnType = BinaryColumn

        override def convert(value: Any): ParquetType
            = if (value == null) null else Binary.fromString(value.asInstanceOf[String])
        override def makeColumn(name: String): ColumnType = FilterApi.binaryColumn(name)
    }

    def filter(name : String, value : Any, schema : StructType, filter : LtGtFilter) : Option[FilterPredicate]
        = PathKey(name).retrieveFrom(schema).flatMap(tpe => tpe.typeName match {
            case "string" => Some(filter[String](name, value))
            case "int" => Some(filter[Int](name, value)) //TODO currect type string?
            case "long" => Some(filter[Long](name, value))
            case "float" => Some(filter[Float](name, value))
            case "double" => Some(filter[Double](name, value))
            case _ => None // Boolean does not support LtGt
        })
}

sealed trait LtGtFilter {
    def apply[T](name : String, value : Any)(implicit ct : LtGtColumn[T]) : FilterPredicate
}

object LtGtFilter {
    val gtFilter = new LtGtFilter {
        override def apply[T](name : String, value : Any)(implicit ct : LtGtColumn[T]) : FilterPredicate
            = FilterApi.gt[ct.ParquetType,ct.ColumnType](ct.makeColumn(name), ct.convert(value))
    }

    val gtEqFilter = new LtGtFilter {
        override def apply[T](name : String, value : Any)(implicit ct : LtGtColumn[T]) : FilterPredicate
            = FilterApi.gtEq[ct.ParquetType,ct.ColumnType](ct.makeColumn(name), ct.convert(value))
    }

    val ltFilter = new LtGtFilter {
        override def apply[T](name : String, value : Any)(implicit ct : LtGtColumn[T]) : FilterPredicate
            = FilterApi.lt[ct.ParquetType,ct.ColumnType](ct.makeColumn(name), ct.convert(value))
    }

    val ltEqFilter = new LtGtFilter {
        override def apply[T](name : String, value : Any)(implicit ct : LtGtColumn[T]) : FilterPredicate
            = FilterApi.ltEq[ct.ParquetType,ct.ColumnType](ct.makeColumn(name), ct.convert(value))
    }
}

/**
  * Reimplements stuff from org.apache.spark.sql.execution.datasources.parquet because it is private
  */
case class StartWithFilter(start: Binary) extends UserDefinedPredicate[Binary] {
    private val len = start.length

    override def canDrop(stats: Statistics[Binary]): Boolean = {
        val comp = PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR
        val min = stats.getMin
        val minInit = min.slice(0, math.min(len, min.length))
        val max = stats.getMax
        val maxInit = max.slice(0, math.min(len, max.length))
        
        comp.compare(start, minInit) < 0 || comp.compare(maxInit, start) < 0
    }

    override def inverseCanDrop(stats: Statistics[Binary]): Boolean = {
        val comp = PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR
        val min = stats.getMin
        val minInit = min.slice(0, math.min(len, min.length))
        val max = stats.getMax
        val maxInit = max.slice(0, math.min(len, max.length))

        comp.compare(start, minInit) >= 0 && comp.compare(maxInit, start) >= 0
    }

    override def keep(value: Binary): Boolean = {
        if (value.length < len) false
        else {
            val comp = PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR
            comp.compare(value.slice(0, len), start) == 0
        }
    }
}

object StartWithFilter {
    def apply(start : String) = new StartWithFilter(Binary.fromString(start))

    def filter(name : String, value : String, schema : StructType) : Option[FilterPredicate] = {
        assert(PathKey(name).retrieveFrom(schema).map(tpe => tpe.typeName == "string").getOrElse(false))
        val col = FilterApi.binaryColumn(name)
        Some(FilterApi.userDefined(col, StartWithFilter(value)))
    }
}
