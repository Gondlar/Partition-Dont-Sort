package de.unikl.cs.dbis.waves.util.nested

import org.apache.spark.sql.types.{DataType, StructType, ArrayType, MapType}

/**
  * A visitor to process the traversal of a nested StructType.
  */
trait DataTypeVisitor {

    /**
      * This callback is called when a Struct is encountered.
      *
      * @param row the row's schema
      */
    def visitStruct(row : StructType) : Unit

    /**
      * This callback is called when a list is encountered.
      * 
      * Since lists are effectively leafs for all cases which do not wish to
      * inspect them, this method defaults to [[visitLeaf]].
      *
      * @param list the list's schema
      */
    def visitList(list : ArrayType) : Unit = visitLeaf(list)

    /**
      * This callback is called when a map is encountered.
      * 
      * Since maps are effectively leafs for all cases which do not wish to
      * inspect them, this method defaults to [[visitLeaf]].
      *
      * @param map the map's schema
      */
    def visitMap(map : MapType) : Unit = visitLeaf(map)

    /**
      * This callback is called when a non-nested datatype is encountered.
      * 
      * [[visitList]] and [[visitMap]] have a default implementation which
      * calls this method. If you do not like this behaviour, override these
      * methods.
      *
      * @param leaf the type
      */
    def visitLeaf(leaf : DataType) : Unit
}

/**
  * Mixin for visitors which supply a single result
  */
trait SingleResult[Type] extends DataTypeVisitor {
  def result: Type
}