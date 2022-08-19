package de.unikl.cs.dbis.waves.split.recursive

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType

import de.unikl.cs.dbis.waves.util.nested.schemas._

/**
  * A Metric represents an assignment of a per-node value. 
  */
trait Metric {
    def measure(results : Array[Int])
}

// This design is an ugly quick-and-dirty solution.
// The main difficulty in finding a good represenation for metrics is that they
// need to bridge the gap between the recursive structure of a schema-tree and a 
// linear array. Depending on the computation, the order in which nodes are
// processed varies.
// A better design define pre/post-order visitors for schemas and rows whose
// functions also take state from their parent/children and can return a result for
// optional nodes. From the outside, they can be used by an Iterator to provide values
// in the respective order and hides the logic of doing the actual traversal.
// This results in a nice separaton of metric calculation and storage.

/**
  * The PresentMetric measures whether a given node is present or not.
  * The value of all present nodes is 1, all absent nodes are 0.
  *
  * @param subject The row to measure
  */
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
                    val skip = field.dataType.optionalNodeCount
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

/**
  * The SwitchMetric determines whether the presence state of corresponding nodes in two documents differs
  *
  * @param lhs The ObjectCounter storing the first document's [[PresentMetric]].
  * @param rhs The ObjectCounter storing the second document's [[PresentMetric]].
  */
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

/**
  * The leaf metric determines how many optional leafs are in the schema subtree rooted in the respective node
  *
  * @param subject the schema 
  */
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
