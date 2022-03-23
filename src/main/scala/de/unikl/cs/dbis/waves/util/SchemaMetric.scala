package de.unikl.cs.dbis.waves.util

import scala.collection.mutable.Map

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

case class SchemaMetric[T](
    value: T,
    children: Map[String, SchemaMetric[T]] = Map.empty[String, SchemaMetric[T]]
) {
    def leafCounts : SchemaMetric[Int] = {
        if (children.isEmpty) {
            SchemaMetric(1, Map.empty[String, SchemaMetric[Int]])
        } else {
            val childcounts = children.map({
                case (name, submetric) => (name, submetric.leafCounts)
            })
            val count = childcounts.map(_._2.value).sum
            SchemaMetric(count, childcounts)
        }
    }

    // This assumes both metrics stem from the same schema.
    // More specifically, this.children.contains(a) <=> other.children.contains(a)
    def combine[O, R](other: SchemaMetric[O], combiner : (T, O) => R) : SchemaMetric[R] = {
        val combinedVal = combiner(value, other.value)
        val combinedChildren = children.map({
            case (name, submetric) => (name, submetric.combine(other.children(name), combiner))
        })
        SchemaMetric(combinedVal, combinedChildren)
    }

    def mapValues[R](f : T => R) : SchemaMetric[R] = {
        val mappedChildren = children.map({
            case (name, submetric) => (name, submetric.mapValues(f))
        })
        SchemaMetric(f(value), mappedChildren)
    }

    def toSeq : Seq[(T, PathKey)] = {
        children.toSeq.flatMap({ case (name, submetric) => {
            submetric.toSeq.map({case (v, path) => {
                (v, path.prepend(name))
            }})
        }}) :+ (value, PathKey(Seq.empty))
    }
}

object SchemaMetric {
    def apply(schema: StructType, row: Option[Row]) : SchemaMetric[Boolean] = {
        val children = Map.empty[String, SchemaMetric[Boolean]]
        for (element <- schema.fields) {
            val submetric = if (element.dataType.isInstanceOf[StructType]) {
                SchemaMetric( element.dataType.asInstanceOf[StructType]
                            , row.map(r => r.getStruct(r.fieldIndex(element.name)))
                                 .filter(_ != null))
            } else {
                SchemaMetric(row.map(r => {
                    assert(r != null)
                    val name = element.name
                    val index = r.fieldIndex(name)
                    r.isNullAt(index)
                }).getOrElse(true))
            }
            children += ((element.name, submetric))
        }
        SchemaMetric(row.isEmpty, children)
    }

    def apply(row: Row) : SchemaMetric[Boolean] = SchemaMetric(row.schema, Some(row))

    def not(metric : SchemaMetric[Boolean]) : SchemaMetric[Boolean] = metric.mapValues(!_)

    // This assumes both metrics stem from the same schema.
    // More specifically, lhs.children.contains(a) <=> rhs.children.contains(a)
    def switch(lhs: SchemaMetric[Boolean], rhs: SchemaMetric[Boolean]) : SchemaMetric[Boolean] = {
        (lhs.value, rhs.value) match {
            // One side is missing, we have a switch wherever the other side is not missing
            case (false, true) => SchemaMetric.not(lhs)
            case (true, false) => SchemaMetric.not(rhs)
            // Both sides are missing. Since all of there children must be missing as well, and all values
            // in children are true. Thus, they do not switch as well and we need an all false metric. The
            // fastest way to obtain one is to negate one of the all true trees 
            case (true, true) => SchemaMetric.not(lhs)
            // Both sides are present, we need to recurse to find switches
            case (false, false) => {
                val children = lhs.children.map({
                    case (name, submetric) => (name, SchemaMetric.switch(submetric, rhs.children(name)))
                })
                SchemaMetric(false, children)
            }
        }
    }

    def presentToCount(obj: SchemaMetric[Boolean]) : SchemaMetric[Int]
        = obj.mapValues(if (_) 1 else 0)

    def sum(lhs: SchemaMetric[Int], rhs: SchemaMetric[Int]) : SchemaMetric[Int]
        = lhs.combine(rhs, (a, b : Int) => a + b)

    def mult(lhs: SchemaMetric[Int], rhs: SchemaMetric[Int]) : SchemaMetric[Int]
        = lhs.combine(rhs, (a, b : Int) => a * b)
    
    def scale(obj: SchemaMetric[Int]) : SchemaMetric[Int]
        = SchemaMetric.mult(obj, obj.leafCounts)

    def missingMetric(data: Array[Row], knownAbsent : Seq[PathKey], knownPresent: Seq[PathKey]) = {
        val counts = data.map(row => SchemaMetric.presentToCount(SchemaMetric(row)))
                         .reduce(SchemaMetric.sum(_,_))
        SchemaMetric.getPaths(counts, knownAbsent, knownPresent)
    }

    def switchMetric(data: Array[Row], knownAbsent : Seq[PathKey], knownPresent: Seq[PathKey]) = {
        val present = data.map(SchemaMetric(_))
        var prev = present.head
        val counts = (for (row <- present.tail) yield {
            val switchcount = SchemaMetric.presentToCount(SchemaMetric.switch(prev, row))
            prev = row
            switchcount
        }).reduce(SchemaMetric.sum(_,_))
        SchemaMetric.getPaths(counts, knownAbsent, knownPresent)
    }

    private def lt(lhs: (Int, PathKey), rhs: (Int, PathKey)) : Boolean = {
        val (v1, p1) = lhs
        val (v2, p2) = rhs
        val diff = v2 - v1
        if (diff < 0) false
        else if (diff > 0) true
        else p2.contains(p1)
    }

    private def getPaths(metric : SchemaMetric[Int], knownAbsent : Seq[PathKey], knownPresent : Seq[PathKey]) = {
        val filtered = SchemaMetric.scale(metric).toSeq.filter({
            case (_, path) => !knownAbsent.map(_.contains(path)).fold(false)(_||_) && !knownPresent.map(_==path).fold(false)(_||_)
        })
        filtered.sortWith(SchemaMetric.lt(_,_)).reverse
    }
}
