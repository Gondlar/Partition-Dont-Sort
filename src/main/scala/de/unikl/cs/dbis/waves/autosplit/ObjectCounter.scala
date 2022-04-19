package de.unikl.cs.dbis.waves.autosplit

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import de.unikl.cs.dbis.waves.util.PathKey

class ObjectCounter private (
    private[autosplit] val values : Array[Int]
) {
    def combine(other : ObjectCounter, f : (Int, Int) => Int) = {
        require(other.values.length == values.length)
        for (i <- 0 to values.length-1) {
            values(i) = f(values(i), other.values(i))
        }
    }

    def map(f : Int => Int) = {
        for (i <- 0 to values.length) {
            values(i) = f(values(i))
        }
    }

    def +=(other : ObjectCounter) = combine(other, _+_)
    def -=(other : ObjectCounter) = combine(other, _-_)
    def *=(other : ObjectCounter) = combine(other, _*_)
    def +=(scalar : Int) = map(_+scalar)
    def -=(scalar : Int) = map(_-scalar)
    def *=(scalar : Int) = map(_*scalar)
    def <--(other : Metric) = other.measure(values)
}

object ObjectCounter {
    def apply(len : Int) : ObjectCounter = new ObjectCounter(Array.fill(len)(0))
    def apply(schema : StructType) : ObjectCounter = ObjectCounter(countOptional(schema))

    private[autosplit] def countOptional(schema : StructType) : Int = {
        schema.fields.map(field => {
            val self = if (field.nullable) 1 else 0
            if (field.dataType.isInstanceOf[StructType]) {
                countOptional(field.dataType.asInstanceOf[StructType]) + self
            } else self
        }).sum
    }

    def paths(schema : StructType) : Seq[PathKey] = {
        schema.fields.flatMap(field => {
            val name = field.name
            val head = if (field.nullable) Seq(PathKey(name)) else Seq.empty
            val tail = if (field.dataType.isInstanceOf[StructType]) paths(field.dataType.asInstanceOf[StructType]) else Seq.empty
            head ++ tail.map(_.prepend(name))
        })
    }
}
