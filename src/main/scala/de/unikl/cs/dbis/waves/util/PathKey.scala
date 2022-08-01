package de.unikl.cs.dbis.waves.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType

/**
  * This class represents a Path into a structured object
  *
  * @param identifiers The steps of the path. There must me at least one step.
  */
final case class PathKey(identifiers: Seq[String]) {
    assert(identifiers.nonEmpty)

    /**
      * represents this PathKey as the string it would be represented as in Spark
      * There, steps are separated by '.'. Not that this differs, e.g., from the 
      * JSONPath specification.
      *
      * @return the string representation
      */
    override def toString = identifiers.mkString(".")

    /**
      * Compute the maximum value of the definition level of the value represented by this path.
      * This corresponds to the number of steps in the path.
      * 
      * Note that the that value may differ from the definition level recorded in Parquet because
      * we count all steps rather than only optional ones.
      *
      * @return the definition level
      */
    def maxDefinitionLevel = identifiers.size

    /**
      * The identifier of the first step in the path
      *
      * @return the identifier
      */
    def head = identifiers.head

    /**
      * Calculate a PathKey without this key's first step, i.e., the path to the same node relative to
      * this path's first step.
      * 
      * A call to this method is only valid if this PathKey is nested
      *
      * @return the path key
      */
    def tail = PathKey(identifiers.tail)


    /**
      * Whether this PathKey represents a nested node, i.e., the path has more than one step
      *
      * @return true iff this PathKey is nested, else false
      */
    def isNested = identifiers.size > 1

    /**
      * Create a new PathKey which has the given identifier as its additional first step.
      * In other words, if this is a path key relative to step, we return the PathKey relative
      * to its parent
      *
      * @param step the identifier to prepend
      * @return the resulting PathKey
      */
    def prepend(step : String) = PathKey(step +: identifiers)

    /**
      * Check whether this PathKey is a prefix of the given PathKey
      *
      * @param other the other PathKey
      * @return true iff this is a prefix of other, else false
      */
    def contains(other : PathKey) : Boolean = {
        if (other.maxDefinitionLevel < maxDefinitionLevel) false else {
            for (index <- 0 to identifiers.size-1) {
                if (identifiers(index) != other.identifiers(index))
                    return false
            }
            true
        }
    }

    /**
      * Check whether this PathKey refers to the same node as the given one, i.e.,
      * chech whether all steps in the paths are equal
      *
      * @param other the PathKey to compare to
      * @return true iff the PathKeys are equal, otherwise false
      */
    def equals(other : PathKey) : Boolean = {
        if (identifiers.length == other.identifiers.length) contains(other)
        else false
    }

    /**
      * Retrieve the node referred to by this PathKey in the given Row.
      * 
      * If the node is absent (because it or one of its ancestors is optional)
      * return its definition level instead. The definition level is the number
      * steps in the path that were actually present. Note that the that value
      * may differ from the definition level recorded in Parquet because we count
      * all steps rather than only optional ones.
      *
      * @param row The row to retrieve data from
      * @return The definition level if the referenced node is absent, otherwise
      *         its data.
      * @throws IllegalArgumentException if this path is not part of the row's schema
      */
    def retrieveFrom(row: Row) : Either[Int,Any] = {
        var currentRow = row
        var res : Option[Any] = Option.empty
        var definitionLevel = 0
        for (step <- identifiers) {
            currentRow.get(currentRow.fieldIndex(step)) match {
                case null => return Left(definitionLevel)
                case subrow: Row => {
                    currentRow = subrow
                    definitionLevel += 1
                }
                case value => {
                    currentRow = null
                    definitionLevel += 1
                    res = Some(value)
                }
            }
        }
        Right(res.orElse(Some(currentRow)).get)
    }

    /**
      * Retrieve the schema element referenced by this key
      *
      * @param tpe the schema
      * @return the referenced schema element iff it is part of the schema
      *         otherwise, None. 
      */
    def retrieveFrom(tpe : StructType) : Option[DataType] = {
        var currentType : Option[DataType] = Some(tpe)
        for (step <- identifiers) {
            currentType = currentType.flatMap(tpe => {
                try {
                    val struct = tpe.asInstanceOf[StructType]
                    Some(struct.fieldIndex(step))
                } catch {
                    case e: IllegalArgumentException => None
                    case e: ClassCastException => None
                }
            }).map(index => currentType.get.asInstanceOf[StructType].fields(index).dataType)
        }
        currentType
    }
}

object PathKey {
    def apply(key: String) : PathKey = PathKey(key.split('.').toSeq)
}
