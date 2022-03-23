package de.unikl.cs.dbis.waves.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType

final case class PathKey(identifiers: Seq[String]) {
    //assert(!identifiers.empty)

    override def toString = identifiers.mkString(".")

    def maxDefinitionLevel = identifiers.size

    def head = identifiers.head
    def tail = PathKey(identifiers.tail)
    def hasSteps = identifiers.size > 1

    def prepend(step : String) = PathKey(step +: identifiers)

    def contains(other : PathKey) : Boolean = {
        if (other.maxDefinitionLevel < maxDefinitionLevel) false else {
            for (index <- 0 to identifiers.size-1) {
                if (identifiers(index) != other.identifiers(index))
                    return false
            }
            true
        }
    }

    def equals(other : PathKey) : Boolean = {
        if (identifiers.length == other.identifiers.length) contains(other)
        else false
    }

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
