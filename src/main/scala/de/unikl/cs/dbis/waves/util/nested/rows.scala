package de.unikl.cs.dbis.waves.util.nested

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StructType, MapType, ArrayType}

object rows {

    /**
      * Extensions for InternalRow that make it easier to work with it from
      * within our framework
      */
    implicit final class InternalRowAccepter(row : InternalRow) {

        /**
          * Accept a visitor. This way, the plumbing for recursively processing
          * an [[InternalRow]] can be hidden away.
          *
          * @param visitor the visitor
          * @param tpe the type of this internal row
          * @throws NullPointerException if row is null because the root row
          *                              must never be missing. Also throws this
          *                              exception if visitor is null
          */
        def accept(visitor : InternalRowVisitor, tpe : StructType)
            = if (row == null) throw new NullPointerException("visited InternalRow was null")
              else visitor.visitStruct(row)(tpe)

        /**
          * This method is usually called from within a [[InternalRowVisitor]]
          * and effectively calls [[accept]] on the object identified by index,
          * but hides away the complexities of accessing both the data and its
          * type. It also handles the special casee of missing values.
          *
          * @param index the index to navigate to
          * @param visitor the visitor to accept
          * @param tpe the current struct's type
          */
        def subAccept(index : Int, visitor : InternalRowVisitor)(implicit tpe : StructType) : Unit = {
            val subTpe = tpe.fields(index).dataType
            if (row.isNullAt(index)) visitor.visitMissing()(subTpe)
            else subTpe match {
                case struct : StructType => visitor.visitStruct(row.getStruct(index, struct.length))(struct)
                case list : ArrayType => visitor.visitList(row.getArray(index))(list)
                case map : MapType => visitor.visitMap(row.getMap(index))(map)
                case _ => visitor.visitLeaf(row.get(index, subTpe))(subTpe)
            }
        }

        /**
          * Shorthand for using [[subAccept]] with a named key instead of a
          * numeric field index
          *
          * @param key the name of the object to navigat to
          * @param visitor the visitor to accept
          * @param tpe the current struct's type
          * @throws IllegalArgumentException when key does not exist in tpe
          */
        def subAccept(key : String, visitor : InternalRowVisitor)(implicit tpe : StructType) : Unit
            = subAccept(tpe.fieldIndex(key), visitor)


        /**
          * This method calls [[subAccept]] on all children from front to back.
          *
          * @param visitor the visitor to accept
          * @param tpe the current struct's type
          */
        def subAcceptAll(visitor : InternalRowVisitor)(implicit tpe : StructType)
            = tpe.fields.indices.foreach(subAccept(_, visitor))

        /**
          * This method calls [[subAccept]] on all children from back to front.
          *
          * @param visitor the visitor to accept
          * @param tpe the current struct's type
          */
        def subAcceptAllRev(visitor : InternalRowVisitor)(implicit tpe : StructType)
            = tpe.fields.indices.reverse.foreach(subAccept(_, visitor))
    }
}
