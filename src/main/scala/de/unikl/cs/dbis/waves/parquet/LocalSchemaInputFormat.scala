package de.unikl.cs.dbis.waves.parquet

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.execution.datasources.PushableColumnAndNestedColumn

import org.apache.parquet.filter2.predicate.{FilterApi,FilterPredicate}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.ShouldNeverHappenException

import de.unikl.cs.dbis.waves.PartitionFolder
import de.unikl.cs.dbis.waves.util.PathKey

class LocalSchemaInputFormat
extends ParquetInputFormat[Row](classOf[LocalScheaReadSupport])

object LocalSchemaInputFormat {
    def read(sc : SparkContext, globalSchema : StructType, folder : PartitionFolder, projection : Iterable[Int] = null, filters : Iterable[Expression] = Iterable.empty) : RDD[Row] = {
        val conf = new Configuration(sc.hadoopConfiguration)
        conf.set(LocalSchemaRecordMaterializer.CONFIG_KEY_SCHEMA, globalSchema.toDDL)
        sparkToParquetFilter(filters) match {
            case Some(filter) => ParquetInputFormat.setFilterPredicate(conf, filter)
            case None => ()
        }
        if (projection != null) {
            conf.set(LocalSchemaRecordMaterializer.CONFIG_KEY_PROJECTION, projection.mkString(","))
        }
        //TODO predicate pushdown
        sc.newAPIHadoopFile( folder.filename
                           , classOf[LocalSchemaInputFormat]
                           , classOf[Void]
                           , classOf[Row]
                           , conf)
          .values
    }

    def sqlEqFilter(unknownIsFalse : Boolean) : EqNeqFilter
        = if (unknownIsFalse) EqNeqFilter.eqFilter else EqNeqFilter.eqOrNullFilter

    def sparkToParquetFilter(filter : Expression, topLevel : Boolean) : Option[FilterPredicate] = {
        val column = PushableColumnAndNestedColumn
        val res = filter match {
            case And(left, right) => for { lhs <- sparkToParquetFilter(left, topLevel)
                                                     ; rhs <- sparkToParquetFilter(right, topLevel)}
                                                 yield FilterApi.and(lhs, rhs)
            case Or(left, right) => for { lhs <- sparkToParquetFilter(left, false)
                                                    ; rhs <- sparkToParquetFilter(right, false)}
                                                yield FilterApi.or(lhs, rhs)
            case Not(child) => sparkToParquetFilter(child, false).map(filter => FilterApi.not(filter))
            case EqualTo(c@column(name), Literal(v, t)) => EqNeqColumn.filter(name, convertToScala(v,t), c.dataType, sqlEqFilter(topLevel))
            case EqualTo(Literal(v, t), c@column(name)) => EqNeqColumn.filter(name, convertToScala(v,t), c.dataType, sqlEqFilter(topLevel))
            case EqualNullSafe(c@column(name), Literal(v, t)) => EqNeqColumn.filter(name, convertToScala(v,t), c.dataType, EqNeqFilter.eqFilter)
            case EqualNullSafe(Literal(v, t), c@column(name)) => EqNeqColumn.filter(name, convertToScala(v,t), c.dataType, EqNeqFilter.eqFilter)
            case IsNull(c@column(name)) => EqNeqColumn.filter(name, null, c.dataType, EqNeqFilter.eqFilter) //TODO: NullIntolerant transformations
            case IsNotNull(c@column(name)) => EqNeqColumn.filter(name, null, c.dataType, EqNeqFilter.notEqFilter)
            case In(c@column(name), values) if values.forall(_.isInstanceOf[Literal]) => {
                val dataType = c.dataType
                val convert = CatalystTypeConverters.createToScalaConverter(dataType)
                values.map(_ match {
                    case Literal(v, _) => EqNeqColumn.filter(name, convert(v), dataType, sqlEqFilter(topLevel))
                    case _ => throw new ShouldNeverHappenException("if above assures all expressions are Literals")
                }).reduce(for {lhs <- _; rhs <- _} yield FilterApi.or(lhs, rhs))
            }
            case InSet(c@column(name), values) => {
                val dataType = c.dataType
                val convert = CatalystTypeConverters.createToScalaConverter(dataType)
                values.map(v => EqNeqColumn.filter(name, convert(v), dataType, sqlEqFilter(topLevel)))
                      .reduce(for {lhs <- _; rhs <- _} yield FilterApi.or(lhs, rhs))
            }
            case GreaterThan(c@column(name), Literal(v, t)) => LtGtColumn.filter(name, convertToScala(v,t), c.dataType, LtGtFilter.gtFilter)
            case GreaterThan(Literal(v, t), c@column(name)) => LtGtColumn.filter(name, convertToScala(v,t), c.dataType, LtGtFilter.ltFilter)
            case LessThan(c@column(name), Literal(v, t)) => LtGtColumn.filter(name, convertToScala(v,t), c.dataType, LtGtFilter.ltFilter)
            case LessThan(Literal(v, t), c@column(name)) => LtGtColumn.filter(name, convertToScala(v,t), c.dataType, LtGtFilter.gtFilter)
            case GreaterThanOrEqual(c@column(name), Literal(v, t)) => LtGtColumn.filter(name, convertToScala(v,t), c.dataType, LtGtFilter.gtEqFilter)
            case GreaterThanOrEqual(Literal(v, t), c@column(name)) => LtGtColumn.filter(name, convertToScala(v,t), c.dataType, LtGtFilter.ltEqFilter)
            case LessThanOrEqual(c@column(name), Literal(v, t)) => LtGtColumn.filter(name, convertToScala(v,t), c.dataType, LtGtFilter.ltEqFilter)
            case LessThanOrEqual(Literal(v, t), c@column(name)) => LtGtColumn.filter(name, convertToScala(v,t), c.dataType, LtGtFilter.gtEqFilter)
            case StartsWith(c@column(name), Literal(v, t)) => StartWithFilter.filter(name, convertToScala(v,t).toString(), c.dataType)
            case _ => None
        }
        println(s"Converted $filter to $res")
        res
    }

    def sparkToParquetFilter(filters : Iterable[Expression]) : Option[FilterPredicate] = {
        println(s"Pushed Filters: ${filters.mkString(" + ")}")
        val parquetFilters = filters.flatMap(f => sparkToParquetFilter(f, true))
        if (parquetFilters.isEmpty) None
        else Some(parquetFilters.reduce(FilterApi.and(_,_)))
    }
}
