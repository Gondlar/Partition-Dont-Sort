package de.unikl.cs.dbis.waves.util.nested

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, StructType, ArrayType, MapType}

/**
  * A visitor to process the traversal of a nested InternalRow.
  * 
  * All methods implicitly carry the type of the respective entry. Instead of
  * handling them explicitly, rely on [[InternalRowAccepter.subAccept]] to
  * navigate nested types.
  */
trait InternalRowVisitor {

    /**
      * This callback is called when a Struct is encountered.
      *
      * @param row the struct's data
      * @param tpe the struct's schema
      */
    def visitStruct(row: InternalRow)(implicit tpe : StructType) : Unit

    /**
      * This callback is called when a list is encountered.
      * 
      * Since lists are effectively leafs for all cases which do not wish to
      * inspect them, this method defaults to [[visitLeaf]].
      *
      * @param list the list's data
      * @param tpe the list's schema
      */
    def visitList(list : ArrayData)(implicit tpe : ArrayType) : Unit = visitLeaf(list)

    /**
      * This callback is called when a map is encountered.
      * 
      * Since maps are effectively leafs for all cases which do not wish to
      * inspect them, this method defaults to [[visitLeaf]].
      *
      * @param map the map's data
      * @param tpe the map's schema
      */
    def visitMap(map : MapData)(implicit tpe : MapType) : Unit = visitLeaf(map)

    /**
      * This callback is called when a non-nested datatype is encountered.
      * 
      * [[visitList]] and [[visitMap]] have a default implementation which
      * calls this method. If you do not like this behaviour, override these
      * methods.
      *
      * @param leaf the data
      * @param tpe the schema
      */
    def visitLeaf(leaf : Any)(implicit tpe : DataType) : Unit

    /**
      * This callback is called when a null entry is encountered
      *
      * @param tpe the type of the missing entry
      */
    def visitMissing()(implicit tpe: DataType) : Unit
}