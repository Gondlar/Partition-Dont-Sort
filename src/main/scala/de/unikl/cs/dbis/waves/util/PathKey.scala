package de.unikl.cs.dbis.waves.util

import org.apache.spark.sql.Row

final case class PathKey(identifiers: Seq[String]) {
    //assert(!identifiers.empty)

    override def toString = identifiers.mkString(".")

    def maxDefinitionLevel = identifiers.size

    def head = identifiers.head
    def tail = PathKey(identifiers.tail)
    def hasSteps = identifiers.size > 1

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
}

object PathKey {
    def apply(key: String) : PathKey = PathKey(key.split('.').toSeq)
}