package de.unikl.cs.dbis.waves.autosplit

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType

trait Metric {
    def measure(results : Array[Int])
}

case class PresentMetric(subject : Row) extends Metric {
    override def measure(results: Array[Int]): Unit = {
        val lastIndex = setToPresenceIn(subject, results, 0)
        assert(lastIndex == results.length)
    }

    private def setToPresenceIn(row : Row, results : Array[Int], offset : Int) : Int = {
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
        val (pos, _) = countLeafs(subject, false, results.length-1, results)
        assert(pos == -1)
    }

    private def countLeafs(schema : DataType, nullable : Boolean, pos: Int, results: Array[Int]) : (Int, Int) = {
        var resultPos = pos
        var count = 0
        schema match {
            case StructType(fields) => {
                for (field <- fields.reverseIterator) {
                    val (newPos, partCount) = countLeafs(field.dataType, field.nullable, resultPos, results)
                    count += partCount
                    resultPos = newPos
                }
            }
            case _ => {
                count = 1
            }
        }
        if (nullable) {
            results(resultPos) = count
            resultPos -=1
        }
        (resultPos, count)
    }
}
