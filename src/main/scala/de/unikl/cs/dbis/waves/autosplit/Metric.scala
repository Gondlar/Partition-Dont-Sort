package de.unikl.cs.dbis.waves.autosplit

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

trait Metric {
    def measure(results : Array[Int])
}

case class PresentMetric(subject : Row) extends Metric {
    override def measure(results: Array[Int]): Unit = {
        val lastIndex = setToPresenceIn(subject, results, 0)
        assert(lastIndex == results.length)
    }

    private def setToPresenceIn(row : Row, results : Array[Int], offset : Int) : Int = {
        assert(offset < results.length)
        var index = offset
        val schema = row.schema
        for (fieldIndex <- 0 to schema.length-1) {
            val field = schema(fieldIndex)
            val isNull = row.isNullAt(fieldIndex)
            if (field.nullable) {
                results(index) = if (isNull) 0 else 1
                index += 1
            }
            if(field.dataType.isInstanceOf[StructType]) {
                if (isNull) {
                    val skip = ObjectCounter.countOptional(field.dataType.asInstanceOf[StructType])
                    for (i <- index to index+skip-1) {
                        results(i) = 0
                    }
                    index += skip
                } else {
                    index = setToPresenceIn(row.getStruct(fieldIndex), results, index)
                }
            }
        }
        index
    }
}

case class SwitchMetric(
    lhs : ObjectCounter,
    rhs : ObjectCounter
) extends Metric {
    override def measure(results: Array[Int]): Unit = {
        require(lhs.values.length == rhs.values.length)
        require(results.length == rhs.values.length)
        for (i <- 0 to results.length-1) {
            results(i) = if (lhs.values(i) == rhs.values(i)) 0 else 1
        }
    }
}

case class LeafMetric(subject : StructType) extends Metric {
    override def measure(results: Array[Int]): Unit = {
        val next = countLeafs(results.length-1, results, subject)
        assert(next == -1)
    }

    private def countLeafs(offset : Int, results: Array[Int], schema : StructType) : Int = {
        assert(offset >= 0 && offset < results.length)
        var pos = offset
        var leafs = 0
        for (field <- schema.fields.reverseIterator) {
            field.dataType match {
                case child : StructType => {
                    pos = countLeafs(pos, results, child)
                    leafs += results(pos+1)
                }
                case _ => {
                    leafs += 1
                    if (field.nullable) {
                        results(pos) = 1
                        pos -= 1
                    }
                }
            }
        }
        results(pos) = leafs
        pos - 1
    }
}
