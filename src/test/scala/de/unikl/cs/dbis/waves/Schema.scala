package de.unikl.cs.dbis.waves

import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.scalatest.Suite

trait Schema extends BeforeAndAfterEach { this: Suite =>

  var schema : StructType = null
  var innerSchema : StructType = null
  var data : Seq[GenericRowWithSchema] = null

  var innerSchemaNonoptional : StructType = null
  var schemaNonoptional : StructType = null
    
  def optionalValue(present: Boolean, value : Any = 5) = if (present) value else null

  protected def makeEntry(a: Boolean, b: Boolean, d: Boolean) = {
      // Inner Row
      val inner = new GenericRowWithSchema(Array[Any](1, optionalValue(d)), innerSchema)
      new GenericRowWithSchema(Array[Any](optionalValue(a), optionalValue(b, inner), 42), schema)
  }

  protected def makeEntry = {
      val inner = new GenericRowWithSchema(Array[Any](1, 5), innerSchemaNonoptional)
      new GenericRowWithSchema(Array[Any](5, inner, 42), schemaNonoptional)
  }

  override def beforeEach() {
    innerSchema = StructType(Seq( StructField("c", IntegerType, false)
                                , StructField("d", IntegerType, true)
                                ))
    schema = StructType(Seq( StructField("a", IntegerType, true)
                           , StructField("b", innerSchema, true)
                           , StructField("e", IntegerType, false)
                           ))
    data = Seq( makeEntry(true,  true,  true)
              , makeEntry(true,  true,  false)
              , makeEntry(true,  false, true)
              , makeEntry(true,  false, false)
              , makeEntry(false, true,  true)
              , makeEntry(false, true,  false)
              , makeEntry(false, false, true)
              , makeEntry(false, false, false)
              )
    innerSchemaNonoptional = StructType(Seq( StructField("c", IntegerType, false)
                                           , StructField("d", IntegerType, false)
                                           ))
    schemaNonoptional = StructType(Seq( StructField("a", IntegerType, false)
                                      , StructField("b", innerSchemaNonoptional, false)
                                      , StructField("e", IntegerType, false)
                                      ))
    super.beforeEach() // To be stackable, must call super.beforeEach
  }
}
