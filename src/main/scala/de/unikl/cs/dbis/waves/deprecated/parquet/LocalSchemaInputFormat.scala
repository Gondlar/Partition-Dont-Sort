package de.unikl.cs.dbis.waves.parquet

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.{ EqualTo
                                    , EqualNullSafe
                                    , GreaterThan
                                    , GreaterThanOrEqual
                                    , LessThan
                                    , LessThanOrEqual
                                    , In
                                    , IsNull
                                    , IsNotNull
                                    , And
                                    , Or
                                    , Not
                                    , StringStartsWith
                                    , StringEndsWith
                                    , StringContains
                                    , AlwaysTrue
                                    , AlwaysFalse
}

import org.apache.parquet.filter2.predicate.{FilterApi,FilterPredicate}
import org.apache.parquet.hadoop.ParquetInputFormat

import de.unikl.cs.dbis.waves.PartitionFolder
import de.unikl.cs.dbis.waves.util.PathKey

class LocalSchemaInputFormat
extends ParquetInputFormat[Row](classOf[LocalScheaReadSupport])

object LocalSchemaInputFormat {
    def read(sc : SparkContext, globalSchema : StructType, folder : PartitionFolder, projection : Array[Int] = null, filters : Array[Filter] = Array.empty) : RDD[Row] = {
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

    def sparkToParquetFilter(filter : Filter, schema : StructType, unknownIsFalse : Boolean) : Option[FilterPredicate] = {
        val res = filter match {
            case EqualTo(attribute, value) => EqNeqColumn.filter(attribute, value, schema, sqlEqFilter(unknownIsFalse))
            case EqualNullSafe(attribute, value) => EqNeqColumn.filter(attribute, value, schema, EqNeqFilter.eqFilter)
            case IsNull(attribute) => EqNeqColumn.filter(attribute, null, schema, EqNeqFilter.eqFilter)
            case IsNotNull(attribute) => EqNeqColumn.filter(attribute, null, schema, EqNeqFilter.notEqFilter)
            case In(attribute, values) => values.map(value => EqNeqColumn.filter(attribute, value, schema, EqNeqFilter.eqFilter))
                                                .reduce((lhs, rhs) => for {lhsVal <- lhs; rhsVal <- rhs} yield FilterApi.or(lhsVal, rhsVal))
            case GreaterThan(attribute, value) => LtGtColumn.filter(attribute, value, schema, LtGtFilter.gtFilter)
            case GreaterThanOrEqual(attribute, value) => LtGtColumn.filter(attribute, value, schema, LtGtFilter.gtEqFilter)
            case LessThan(attribute, value) => LtGtColumn.filter(attribute, value, schema, LtGtFilter.ltFilter)
            case LessThanOrEqual(attribute, value) => LtGtColumn.filter(attribute, value, schema, LtGtFilter.gtEqFilter)
            case StringStartsWith(attribute, value) => StartWithFilter.filter(attribute, value, schema)
            case StringEndsWith(attribute, value) => None //TODO custom implementation?
            case StringContains(attribute, value) => None //TODO custom implementation?
            case And(left, right) => {
                val lhs = sparkToParquetFilter(left, schema, false) 
                val rhs = sparkToParquetFilter(right, schema, false)
                lhs match {
                    case None => rhs
                    case Some(left_filter) => rhs match {
                        case None => lhs
                        case Some(right_filter) => Some(FilterApi.and(left_filter, right_filter))
                    }
                }
            }
            case Or(left, right) => for { lhs <- sparkToParquetFilter(left, schema, false)
                                        ; rhs <- sparkToParquetFilter(right, schema, false)}
                                    yield FilterApi.or(lhs, rhs)
            case Not(child) => sparkToParquetFilter(child, schema, false).map(filter => FilterApi.not(filter))
            case AlwaysTrue() => None
            case AlwaysFalse() => None //TODO can't be encoded but technically, we can skip reading, if we encounter this toplevel?
        }
        println(s"Converted $filter to $res")
        res
    }

    def sparkToParquetFilter(filters : Array[Filter], schema : StructType) : Option[FilterPredicate] = {
        println(s"Pushed Filters: ${filters.mkString(" + ")}")
        val parquetFilters = filters.flatMap(f => sparkToParquetFilter(f, schema, true))
        if (parquetFilters.isEmpty) None
        else Some(parquetFilters.reduce((lhs, rhs) => FilterApi.and(lhs, rhs)))
    }
}
