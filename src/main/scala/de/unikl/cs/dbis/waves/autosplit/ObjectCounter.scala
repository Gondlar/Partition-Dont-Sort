package de.unikl.cs.dbis.waves.autosplit

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import de.unikl.cs.dbis.waves.util.PathKey

/**
  * An ObjectCounter holds an Integer value for every optional node in a JSON document.
  * 
  * Do not call this constructor directly! Use the apply-methods.
  *
  * @param values An array which holds the values for each node.
  *               The array index corresponds to an optional node's position in pre-order. 
  */
  @SerialVersionUID(100L)
class ObjectCounter private[autosplit] (
    private[autosplit] val values : Array[Int]
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
    def apply(schema : StructType) : ObjectCounter = ObjectCounter(countOptional(schema))

    /**
      * Determine how many optional nodes a schema has
      *
      * @param schema the schema
      * @return its number of optional nodes
      */
    private[autosplit] def countOptional(schema : StructType) : Int = {
        schema.fields.map(field => {
            val self = if (field.nullable) 1 else 0
            if (field.dataType.isInstanceOf[StructType]) {
                countOptional(field.dataType.asInstanceOf[StructType]) + self
            } else self
        }).sum
    }

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
            head ++ tail.map(_.prepend(name))
        })
    }
}
