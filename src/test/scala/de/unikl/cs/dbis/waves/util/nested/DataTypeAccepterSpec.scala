package de.unikl.cs.dbis.waves.util.nested

import schemas._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SchemaFixture
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType

class DataTypeAccepterSpec extends WavesSpec 
    with SchemaFixture {

    "A DataTypeAccepter" when {
        "accepting a schema" should {
            "call visitStruct" in {
                schema.accept(new DataTypeVisitor {
                    override def visitStruct(row: StructType): Unit
                        = row should equal (schema)

                    override def visitLeaf(leaf: DataType): Unit
                        = fail
                })
            }
            "throw an exception for missing schemas" in {
                val foo : DataType = null
                val visitor = new DataTypeVisitor {
                    override def visitStruct(row: StructType): Unit = fail
                    override def visitLeaf(leaf: DataType): Unit = fail
                }
                an [NullPointerException] should be thrownBy (foo.accept(visitor))
            }
            "throw an exception for missing visitors" in {
                an [NullPointerException] should be thrownBy (schema.accept(null))
            }
        }
    }
    "A StructTypeAccepter" when {
        "accepting a schema" should {
            "call the correct callback using indices (1)" in {
                val visitor = new DataTypeVisitor {
                    override def visitStruct(row: StructType): Unit = fail
                    override def visitLeaf(leaf: DataType): Unit
                        = leaf should equal (schema.fields(0).dataType)
                }
                schema.subAccept(0, visitor)
            }
            "call the correct callback using indices (2)" in {
                val visitor = new DataTypeVisitor {
                    override def visitStruct(row: StructType): Unit
                        = row should equal (schema.fields(1).dataType)
                    override def visitLeaf(leaf: DataType): Unit = fail
                }
                schema.subAccept(1, visitor)
            }
            "call the correct callback using names (1)" in {
                val visitor = new DataTypeVisitor {
                    override def visitStruct(row: StructType): Unit = fail
                    override def visitLeaf(leaf: DataType): Unit = {
                        leaf should equal (schema.fields(0).dataType)
                    }
                }
                schema.subAccept("a", visitor)
            }
            "call the correct callback using names (2)" in {
                val visitor = new DataTypeVisitor {
                    override def visitStruct(row: StructType): Unit = {
                        row should equal (schema.fields(1).dataType)
                    }
                    override def visitLeaf(leaf: DataType): Unit = fail
                }
                schema.subAccept("b", visitor)
            }
            "throw an exception for non-existent names" in {
                val visitor = new DataTypeVisitor {
                    override def visitStruct(row: StructType) : Unit = fail
                    override def visitLeaf(leaf: DataType) : Unit = fail
                }
                an [IllegalArgumentException] should be thrownBy (schema.subAccept("foo", visitor))
            }
            "visit the children in the correct order" in {
                var count = 0
                val visitor = new DataTypeVisitor {
                    override def visitStruct(row: StructType): Unit = visitLeaf(row)
                    override def visitLeaf(leaf: DataType): Unit = {
                        leaf should equal (schema.fields(count).dataType)
                        count += 1
                    }
                }
                schema.subAcceptAll(visitor)
                count should equal (schema.fields.length)
            }
            "visit the children in the correct reverse order" in {
                var count = schema.fields.length
                val visitor = new DataTypeVisitor {
                    override def visitStruct(row: StructType): Unit = visitLeaf(row)
                    override def visitLeaf(leaf: DataType): Unit = {
                        count -= 1
                        leaf should equal (schema.fields(count).dataType)
                    }
                }
                schema.subAcceptAllRev(visitor)
                count should equal (0)
            }
        }
    }
    
}