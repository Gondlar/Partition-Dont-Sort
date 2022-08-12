package de.unikl.cs.dbis.waves.util.nested

import org.apache.spark.sql.types.{DataType, StructType, MapType, ArrayType}

object schemas {
    /**
      * Extensions for [[DataType]] that make it easier to work with from
      * within our framework
      */
    implicit final class DataTypeAccepter(tpe : DataType) {

        /**
          * Accept avisitor. This way, the plumbing for recursively processing
          * a [[DataType]] can be hidden.
          *
          * @param visitor the visitor
          */
        def accept(visitor : DataTypeVisitor) = tpe match {
            case null => throw new NullPointerException("tpe is null")
            case struct : StructType => visitor.visitStruct(struct)
            case list : ArrayType => visitor.visitList(list)
            case map : MapType => visitor.visitMap(map)
            case _ => visitor.visitLeaf(tpe)
        }
    }

    /**
      * Extensions for [[StructType]] that make it easier to work with from
      * within our framework
      */
    implicit final class StructTypeAccepter(tpe : StructType) {

        /**
          * This method is usually called from within a [[DataTypeVisitor]]
          * and effectively calls [[accept]] on the object identified by index.
          *
          * @param index the index to navigate to
          * @param visitor the visitor to accept
          */
        def subAccept(index : Int, visitor : DataTypeVisitor) : Unit
            = tpe.fields(index).dataType.accept(visitor)
        
        /**
          * Shorthand for using [[subAccept]] with a named key instead of a
          * numeric field index
          *
          * @param key the key
          * @param visitor the visitor
          */
        def subAccept(key : String, visitor : DataTypeVisitor) : Unit
            = tpe.subAccept(tpe.fieldIndex(key), visitor)
        
        /**
          * This method calls [[subAccept]] on all children from front to back.
          *
          * @param visitor the visitor
          */
        def subAcceptAll(visitor : DataTypeVisitor) : Unit
            = tpe.fields.foreach(_.dataType.accept(visitor))

        /**
          * This method calls [[subAccept]] on all children from back to front.
          *
          * @param visitor
          */
        def subAcceptAllRev(visitor : DataTypeVisitor) : Unit
            = tpe.fields.reverseIterator.foreach(_.dataType.accept(visitor))
    }
}
