package de.unikl.cs.dbis.waves.split.recursive

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.nested.schemas._

/**
  * An ObjectCounter holds an Integer value for every optional node in a JSON document.
  * 
  * Do not call this constructor directly! Use the apply-methods.
  *
  * @param values An array which holds the values for each node.
  *               The array index corresponds to an optional node's position in pre-order. 
  */
  @SerialVersionUID(100L)
class ObjectCounter private[recursive] (
    private[recursive] val values : Array[Int]
) extends Serializable {
    /**
      * Combine two ObjectCounters by applying a function to the values of two matching nodes.
      * The results override this ObjectCounter's data. The other one stays unchanged.
      *
      * @param other The other ObjectCounter
      * @param f The combination function
      */
    def combine(other : ObjectCounter, f : (Int, Int) => Int) = {
        require(other.values.length == values.length)
        for (i <- 0 to values.length-1) {
            values(i) = f(values(i), other.values(i))
        }
    }

    /**
      * Applys a function to the values for every node
      *
      * @param f The function to be applied
      */
    def map(f : Int => Int) = {
        for (i <- 0 to values.length-1) {
            values(i) = f(values(i))
        }
    }

    // Shorthands
    def +=(other : ObjectCounter) = combine(other, _+_)
    def -=(other : ObjectCounter) = combine(other, _-_)
    def *=(other : ObjectCounter) = combine(other, _*_)
    def +=(scalar : Int) = map(_+scalar)
    def -=(scalar : Int) = map(_-scalar)
    def *=(scalar : Int) = map(_*scalar)

    /**
      * Store the results of the given metric in this ObjectCounter.
      * The effect depends on the implementation of the respective metric.
      *
      * @param other The metric to be applied
      */
    def <--(other : Metric) = other.measure(values)

    /**
      * @return the number of objects this ObjectCounter is keeping track of
      */
    def size = values.length

    /**
      * Produce a map from the given keys to the values of this object counter
      *
      * @param keys the keys, e.g., the paths to the optional nodes
      * @return a map from keys to their values in the counter
      */
    def toMap[A](keys: Seq[A]) = {
      assert(keys.size == size)
      keys.zip(values).toMap
    }

    override def equals(other: Any): Boolean = other match {
      case counter: ObjectCounter => counter.values sameElements values
      case _ => false
    }

    override def toString(): String = s"ObjectCounter(${values.mkString(",")})"
}

object ObjectCounter {
    /**
      * Initialize an ObjectCounter using a known size obtained using [[countOptional]].
      *
      * @param len The known size
      * @return The newly constructed ObjectCounter
      */
    def apply(len : Int) : ObjectCounter = new ObjectCounter(Array.fill(len)(0))

    /**
      * Initialize an ObjectCounter fitting a given schema
      *
      * @param schema The schema to use
      * @return The newly constructed ObjectCounter
      */
    def apply(schema : StructType) : ObjectCounter = ObjectCounter(schema.optionalNodeCount)

    /**
      * Determine the paths of all optional nodes in a schema in pre-order
      *
      * @param schema the schema
      * @return its optional paths
      */
    def paths(schema : StructType) : Seq[PathKey] = {
        schema.fields.flatMap(field => {
            val name = field.name
            val head = if (field.nullable) Seq(PathKey(name)) else Seq.empty
            val tail = if (field.dataType.isInstanceOf[StructType]) paths(field.dataType.asInstanceOf[StructType]) else Seq.empty
            head ++ tail.map(name +: _)
        })
    }
}
