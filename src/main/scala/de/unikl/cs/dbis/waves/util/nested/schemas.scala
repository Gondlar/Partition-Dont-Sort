package de.unikl.cs.dbis.waves.util.nested

import org.apache.spark.sql.types.{DataType, StructType, StructField, MapType, ArrayType}
import de.unikl.cs.dbis.waves.util.PathKey

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

        /**
          * Count the number of leaf nodes in this schema.
          * Arrays and maps are treated as leafs
          *
          * @return the number of leafs
          */
        def leafCount() = {
            var count = 0
            val visitor = new DataTypeVisitor{
                override def visitStruct(row: StructType): Unit = row.subAcceptAll(this)
                override def visitLeaf(leaf: DataType): Unit = count += 1
            }
            tpe.accept(visitor)
            count
        }

        /**
          * Count the number of optional leaf nodes in this schema, i.e., all
          * leafs below an optional node.
          * Arrays and maps are treated as leafs
          *
          * @return the number of leafs
          */
        def optionalLeafCount() = {
            var count = 0
            val visitor = new DataTypeVisitor{
                override def visitStruct(row: StructType): Unit = {
                  for (field <- row) {
                    if (field.nullable) count += field.dataType.leafCount()
                    else field.dataType.accept(this)
                  }
                }
                override def visitLeaf(leaf: DataType): Unit = ()
            }
            tpe.accept(visitor)
            count
        }

        /**
          * Count the number of nodes in this schema.
          * Arrays and maps are treated as leafs
          *
          * @return the number of nodes
          */
        def nodeCount() = {
            var count = 0
            val visitor = new DataTypeVisitor{
                override def visitStruct(row: StructType): Unit = {
                    count += 1
                    row.subAcceptAll(this)
                }
                override def visitLeaf(leaf: DataType): Unit = count += 1
            }
            tpe.accept(visitor)
            count
        }

        /**
          * Count the number of optional (i.e., nullable) nodes in this schema.
          * Note that optional is a property of struct fields, so the DataType
          * you call this method on itself is never included in the count.
          * Arrays and maps are treated as leafs.
          *
          * @return the number of optional nodes
          */
        def optionalNodeCount() = {
          var count = 0
          val visitor = new DataTypeVisitor {
            override def visitStruct(row: StructType): Unit =  {
              count += row.fields.count(_.nullable)
              row.subAcceptAll(this)
            }
            override def visitLeaf(leaf: DataType): Unit = ()
          }
          tpe.accept(visitor)
          count
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

        /**
          * Mark the given path as present in the schema, i.e., all StructFields
          * on the path are not nullable in the result
          *
          * @param key the path to mark as present
          * @return the changed schema
          * @throws IllegalArgumentException if the given path does not fit the
          *                                  schema
          */
        def withPresent(key: PathKey) = {
          var rest: Option[PathKey] = Some(key)
          var result: DataType = null
          val visitor = new DataTypeVisitor {
            override def visitLeaf(leaf: DataType): Unit = rest match {
              case None => result = leaf
              case Some(_) => throw new IllegalArgumentException(s"key \'$key\' does not exist")
            }

            override def visitStruct(row: StructType): Unit = rest match {
              case None => result = row
              case Some(key) => {
                val index = row.fieldIndex(key.head)
                rest = if (key.isNested) Some(key.tail) else None
                row.subAccept(index, this)

                val builder = Array.newBuilder[StructField]
                val fields = row.fields.toSeq
                builder ++= fields.take(index)
                builder += fields(index).copy(dataType = result, nullable = false)
                builder ++= fields.drop(index+1)
                result = StructType(builder.result())
              }
            }
          }
          tpe.accept(visitor)
          result.asInstanceOf[StructType]
        }
    }
}
