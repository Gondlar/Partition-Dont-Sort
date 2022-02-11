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
        sparkToParquetFilter(filters, globalSchema) match {
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

    def sparkToParquetFilter(filter : Expression, schema : StructType, topLevel : Boolean) : Option[FilterPredicate] = {
        val column = PushableColumnAndNestedColumn
        val res = filter match {
            case And(left, right) => for { lhs <- sparkToParquetFilter(left, schema, topLevel)
                                                     ; rhs <- sparkToParquetFilter(right, schema, topLevel)}
                                                 yield FilterApi.and(lhs, rhs)
            case Or(left, right) => for { lhs <- sparkToParquetFilter(left, schema, false)
                                                    ; rhs <- sparkToParquetFilter(right, schema, false)}
                                                yield FilterApi.or(lhs, rhs)
            case Not(child) => sparkToParquetFilter(child, schema, false).map(filter => FilterApi.not(filter))
            case EqualTo(column(name), Literal(v, t)) => EqNeqColumn.filter(name, convertToScala(v,t), schema, sqlEqFilter(topLevel))
            case EqualTo(Literal(v, t), column(name)) => EqNeqColumn.filter(name, convertToScala(v,t), schema, sqlEqFilter(topLevel))
            case EqualNullSafe(column(name), Literal(v, t)) => EqNeqColumn.filter(name, convertToScala(v,t), schema, EqNeqFilter.eqFilter)
            case EqualNullSafe(Literal(v, t), column(name)) => EqNeqColumn.filter(name, convertToScala(v,t), schema, EqNeqFilter.eqFilter)
            case IsNull(column(name)) => EqNeqColumn.filter(name, null, schema, EqNeqFilter.eqFilter) //TODO: NullIntolerant transformations
            case IsNotNull(column(name)) => EqNeqColumn.filter(name, null, schema, EqNeqFilter.notEqFilter)
            case In(c@column(name), values) if values.forall(_.isInstanceOf[Literal]) => {
                val convert = CatalystTypeConverters.createToScalaConverter(c.dataType)
                values.map(_ match {
                    case Literal(v, _) => EqNeqColumn.filter(name, convert(v), schema, sqlEqFilter(topLevel))
                    case _ => throw new ShouldNeverHappenException("if above assures all expressions are Literals")
                }).reduce(for {lhs <- _; rhs <- _} yield FilterApi.or(lhs, rhs))
            }
            case InSet(c@column(name), values) => {
                val convert = CatalystTypeConverters.createToScalaConverter(c.dataType)
                values.map(v => EqNeqColumn.filter(name, convert(v), schema, sqlEqFilter(topLevel)))
                      .reduce(for {lhs <- _; rhs <- _} yield FilterApi.or(lhs, rhs))
            }
            case GreaterThan(column(name), Literal(v, t)) => LtGtColumn.filter(name, convertToScala(v,t), schema, LtGtFilter.gtFilter)
            case GreaterThan(Literal(v, t), column(name)) => LtGtColumn.filter(name, convertToScala(v,t), schema, LtGtFilter.ltFilter)
            case LessThan(column(name), Literal(v, t)) => LtGtColumn.filter(name, convertToScala(v,t), schema, LtGtFilter.ltFilter)
            case LessThan(Literal(v, t), column(name)) => LtGtColumn.filter(name, convertToScala(v,t), schema, LtGtFilter.gtFilter)
            case GreaterThanOrEqual(column(name), Literal(v, t)) => LtGtColumn.filter(name, convertToScala(v,t), schema, LtGtFilter.gtEqFilter)
            case GreaterThanOrEqual(Literal(v, t), column(name)) => LtGtColumn.filter(name, convertToScala(v,t), schema, LtGtFilter.ltEqFilter)
            case LessThanOrEqual(column(name), Literal(v, t)) => LtGtColumn.filter(name, convertToScala(v,t), schema, LtGtFilter.ltEqFilter)
            case LessThanOrEqual(Literal(v, t), column(name)) => LtGtColumn.filter(name, convertToScala(v,t), schema, LtGtFilter.gtEqFilter)
            case StartsWith(column(name), Literal(v, t)) => StartWithFilter.filter(name, convertToScala(v,t).toString(), schema)
            case _ => None
        }
        println(s"Converted $filter to $res")
        res
    }

    def sparkToParquetFilter(filters : Iterable[Expression], schema : StructType) : Option[FilterPredicate] = {
        println(s"Pushed Filters: ${filters.mkString(" + ")}")
        val parquetFilters = filters.flatMap(f => sparkToParquetFilter(f, schema, true))
        if (parquetFilters.isEmpty) None
        else Some(parquetFilters.reduce(FilterApi.and(_,_)))
    }
}
