package de.unikl.cs.dbis.waves.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, DataType}
import org.apache.spark.sql.catalyst.InternalRow

import de.unikl.cs.dbis.waves.util.nested.{InternalRowVisitor,DataTypeVisitor}
import de.unikl.cs.dbis.waves.util.nested.schemas._
import de.unikl.cs.dbis.waves.util.nested.rows._

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
    def toSpark = identifiers.mkString(".")
    override def toString = toSpark

    /**
      * @return A string representation of this path which does not use dot as
      *         the separator
      */
    def toDotfreeString = identifiers.mkString("/")

    /**
      * represent this PathKey as a Spark Column
      */
    def toCol = col(toSpark)

    /**
      * represent this PathKey as a Spark Column bound to the given DataFrame
      */
    def toCol(df: DataFrame) = df.col(toSpark)

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
    def tail = if (identifiers.size == 1) throw new NoSuchElementException
               else PathKey(identifiers.tail)

    /**
      * Calculate the path to this key's parent
      * 
      * A call to this method is only valid if this PathKey is nested
      *
      * @return the path key or none if the path is not nested
      */
    def parent = if (isNested) Some(PathKey(identifiers.init)) else None

    /**
      * Whether this PathKey represents a nested node, i.e., the path has more than one step
      *
      * @return true iff this PathKey is nested, else false
      */
    def isNested = identifiers.size > 1

    /**
      * Create a new PathKey which has the given identifier as its additional final step.
      * In other words, we navigate one step deeper.
      *
      * @param step the identifier to append
      * @return the resulting PathKey
      */
    def :+(step : String) = PathKey(identifiers :+ step)

    /**
      * Create a new PathKey which has the given identifier as its additional first step.
      * In other words, if this is a path key relative to step, we return the PathKey relative
      * to its parent
      *
      * @param step the identifier to prepend
      * @return the resulting PathKey
      */
    def +:(step : String) = PathKey(step +: identifiers)

    /**
      * Check whether this PathKey is a prefix of the given PathKey
      *
      * @param other the other PathKey
      * @return true iff this is a prefix of other, else false
      */
    def isPrefixOf(other : PathKey) : Boolean = {
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
        if (identifiers.length == other.identifiers.length) isPrefixOf(other)
        else false
    }

    /**
      * Retrieve the node referred to by this PathKey in the given Row and its schema.
      *
      * @param row The row to retrieve data from
      * @param schema The schema of that row
      * @return The data if it is present, otherwise None
      * @throws IllegalArgumentException if this path is not part of the row's schema
      */
    def retrieveFrom(row: InternalRow, schema : StructType) : Option[Any] = {
        var step = 0
        var result : Option[Any] = None
        val visitor = new InternalRowVisitor {
            override def visitStruct(row: InternalRow)(implicit tpe: StructType): Unit = {
                if (step < identifiers.length) {
                    val identifier = identifiers(step)
                    step += 1
                    row.subAccept(identifier, this)
                } else result = Some(row)
            }

            override def visitLeaf(leaf: Any)(implicit tpe: DataType): Unit
              = if (step == identifiers.length) result = Some(leaf)

            override def visitMissing()(implicit tpe: DataType): Unit
              = {}
        }
        row.accept(visitor, schema)
        result
    }

    /**
      * Retrieve the node referred to by this PathKey in the given Row and its schema.
      *
      * @param row The row to retrieve data from
      * @param schema The schema of that row
      * @return The data if it is present, otherwise None
      * @throws IllegalArgumentException if this path is not part of the row's schema
      */
    def retrieveFrom(row: Row, schema : StructType) : Option[Any] = {
      val index = schema.fieldIndex(head)
      if (row.isNullAt(index))
        return None
      val next = row.get(index)
      if (!isNested)
        return Some(next)
      schema.fields(index).dataType match {
        case struct: StructType => tail.retrieveFrom(next.asInstanceOf[Row], struct)
        case _ => throw new IllegalArgumentException(s"schema element $head refers to a non-struct element, but the path continues as $tail")
      }
    }

    /**
      * Retrieve the schema element referenced by this key
      *
      * @param tpe the schema
      * @return the referenced schema element iff it is part of the schema
      *         otherwise, None. 
      */
    def retrieveFrom(tpe : StructType) : Option[DataType] = {
        var step = 0
        var result : Option[DataType] = None
        val visitor = new DataTypeVisitor {
            override def visitStruct(row: StructType): Unit = {
                if (step < identifiers.length) {
                    val identifier = identifiers(step)
                    step += 1
                    row.subAccept(identifier, this)
                } else result = Some(row)
            }

            override def visitLeaf(leaf: DataType): Unit
              = if (step == identifiers.length) result = Some(leaf)
        }
        try tpe.accept(visitor)
        catch {case e: IllegalArgumentException => return None}
        result
    }

    def present(row: InternalRow, schema: StructType) = retrieveFrom(row, schema).isDefined
    def present(row: Row, schema: StructType) = retrieveFrom(row, schema).isDefined
}

object PathKey {
    def apply(key: String) : PathKey = PathKey(key.split('.').toSeq)

    /**
      * We represent Path Keys which possibly refer to the root as Option[PathKey]
      * and extend its API with appropriate wrappers
      *
      * @param key
      */
    implicit class RootPathKey(key : Option[PathKey]) {
        def toSpark: String = key.map(_.toSpark).getOrElse("*")
        def toCol = key.get.toCol
        def toCol(df: DataFrame) = key.get.toCol(df)
        def maxDefinitionLevel = key.map(_.maxDefinitionLevel).getOrElse(0)
        def head = key.get.head
        def tail = key match {
          case None => throw new NoSuchElementException
          case Some(value) if !key.isNested => None
          case Some(key) => Some(key.tail)
        }
        def parent = key match {
          case None => throw new NoSuchElementException
          case Some(key) => key.parent
        }
        def isNested = key.map(_.isNested).getOrElse(false)
        def +:(step : String) = key.map(step +: _).orElse(Some(PathKey(step)))
        def :+(step : String) = key.map(_ :+ step).orElse(Some(PathKey(step)))
        def isPrefixOf(other : PathKey): Boolean = key.map(_ isPrefixOf other).getOrElse(true)
        def isPrefixOf(other : Option[PathKey]): Boolean = other match {
            case None => key.isEmpty
            case Some(value) => key isPrefixOf value
        }
        def retrieveFrom(row: InternalRow, schema : StructType) = key.map(_.retrieveFrom(row, schema)).getOrElse(Some(row))
        def retrieveFrom(tpe : StructType) = key.map(_.retrieveFrom(tpe)).getOrElse(Some(tpe))
        def present(row: InternalRow, schema: StructType) = retrieveFrom(row,schema).isDefined
    }
}
