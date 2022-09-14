package de.unikl.cs.dbis.waves.util.nested

import rows._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SchemaFixture
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType

class InternalRowAccepterSpec extends WavesSpec 
    with SchemaFixture {

    "A InternalRowAccepter" when {
        "accepting a row" should {
            "call visitStruct" in {
                internalData(1).accept(new InternalRowVisitor {

                    override def visitStruct(row: InternalRow)(implicit tpe: StructType): Unit = {
                        row should equal (internalData(1))
                        tpe should equal (schema)
                    }

                    override def visitLeaf(leaf: Any)(implicit tpe: DataType): Unit
                        = fail

                    override def visitMissing()(implicit tpe: DataType): Unit
                        = fail


                }, schema)
            }
            "throw an exception for missing root rows" in {
                val foo : InternalRow = null
                val visitor = new InternalRowVisitor {
                    override def visitStruct(row: InternalRow)(implicit tpe: StructType): Unit = fail
                    override def visitLeaf(leaf: Any)(implicit tpe: DataType): Unit = fail 
                    override def visitMissing()(implicit tpe: DataType): Unit = fail
                }
                an [NullPointerException] should be thrownBy (foo.accept(visitor, schema))
            }
            "throw an exception for missing visitors" in {
                an [NullPointerException] should be thrownBy (internalData(1).accept(null, schema))
            }
            "call the correct callback using indices (1)" in {
                val visitor = new InternalRowVisitor {
                    override def visitStruct(row: InternalRow)(implicit tpe: StructType): Unit = fail
                    override def visitLeaf(leaf: Any)(implicit tpe: DataType): Unit = {
                        leaf should equal (5)
                        tpe should equal (schema.fields(0).dataType)
                    }
                    override def visitMissing()(implicit tpe: DataType): Unit = fail
                }
                internalData(0).subAccept(0, visitor)(schema)
            }
            "call the correct callback using indices (2)" in {
                val visitor = new InternalRowVisitor {
                    override def visitStruct(row: InternalRow)(implicit tpe: StructType): Unit = {
                        row should equal (internalData(0).getStruct(
                            1,
                            schema.fields(1).dataType.asInstanceOf[StructType].length
                        ))
                        tpe should equal (schema.fields(1).dataType)
                    }
                    override def visitLeaf(leaf: Any)(implicit tpe: DataType): Unit = fail
                    override def visitMissing()(implicit tpe: DataType): Unit = fail
                }
                internalData(0).subAccept(1, visitor)(schema)
            }
            "call the correct callback using indices (3)" in {
                val visitor = new InternalRowVisitor {
                    override def visitStruct(row: InternalRow)(implicit tpe: StructType): Unit = fail
                    override def visitLeaf(leaf: Any)(implicit tpe: DataType): Unit = fail
                    override def visitMissing()(implicit tpe: DataType): Unit = {
                        tpe should equal (schema.fields(1).dataType)
                    }
                }
                internalData(2).subAccept(1, visitor)(schema)
            }
            "call the correct callback using names (1)" in {
                val visitor = new InternalRowVisitor {
                    override def visitStruct(row: InternalRow)(implicit tpe: StructType): Unit = fail
                    override def visitLeaf(leaf: Any)(implicit tpe: DataType): Unit = {
                        leaf should equal (5)
                        tpe should equal (schema.fields(0).dataType)
                    }
                    override def visitMissing()(implicit tpe: DataType): Unit = fail
                }
                internalData(0).subAccept("a", visitor)(schema)
            }
            "call the correct callback using names (2)" in {
                val visitor = new InternalRowVisitor {
                    override def visitStruct(row: InternalRow)(implicit tpe: StructType): Unit = {
                        row should equal (internalData(0).getStruct(
                            1,
                            schema.fields(1).dataType.asInstanceOf[StructType].length
                        ))
                        tpe should equal (schema.fields(1).dataType)
                    }
                    override def visitLeaf(leaf: Any)(implicit tpe: DataType): Unit = fail
                    override def visitMissing()(implicit tpe: DataType): Unit = fail
                }
                internalData(0).subAccept("b", visitor)(schema)
            }
            "call the correct callback using names (3)" in {
                val visitor = new InternalRowVisitor {
                    override def visitStruct(row: InternalRow)(implicit tpe: StructType): Unit = fail
                    override def visitLeaf(leaf: Any)(implicit tpe: DataType): Unit = fail
                    override def visitMissing()(implicit tpe: DataType): Unit = {
                        tpe should equal (schema.fields(1).dataType)
                    }
                }
                internalData(2).subAccept("b", visitor)(schema)
            }
            "throw an exception for non-existent names" in {
                val visitor = new InternalRowVisitor {
                    override def visitStruct(row: InternalRow)(implicit tpe: StructType): Unit = fail
                    override def visitLeaf(leaf: Any)(implicit tpe: DataType): Unit = fail 
                    override def visitMissing()(implicit tpe: DataType): Unit = fail
                }
                an [IllegalArgumentException] should be thrownBy (internalData(0).subAccept("foo", visitor)(schema))
            }
            "visit the children in the correct order" in {
                var count = 0
                val visitor = new InternalRowVisitor {
                    override def visitStruct(row: InternalRow)(implicit tpe: StructType): Unit = {
                        val actualTpe = schema.fields(count).dataType
                        tpe should equal (actualTpe)
                        row should equal (internalData(1).get(count, actualTpe))
                        count += 1
                    }
                    override def visitLeaf(leaf: Any)(implicit tpe: DataType): Unit = {
                        val actualTpe = schema.fields(count).dataType
                        tpe should equal (actualTpe)
                        leaf should equal (internalData(1).get(count, actualTpe))
                        count += 1
                    }
                    override def visitMissing()(implicit tpe: DataType): Unit = {
                        val actualTpe = schema.fields(count).dataType
                        tpe should equal (actualTpe)
                        internalData(1).get(count, actualTpe) should equal (null)
                        count += 1
                    }
                }
                internalData(1).subAcceptAll(visitor)(schema)
                count should equal (schema.fields.length)
            }
            "visit the children in the correct reverse order" in {
                var count = schema.fields.length
                val visitor = new InternalRowVisitor {
                    override def visitStruct(row: InternalRow)(implicit tpe: StructType): Unit = {
                        count -= 1
                        val actualTpe = schema.fields(count).dataType
                        tpe should equal (actualTpe)
                        row should equal (internalData(1).get(count, actualTpe))
                    }
                    override def visitLeaf(leaf: Any)(implicit tpe: DataType): Unit = {
                        count -= 1
                        val actualTpe = schema.fields(count).dataType
                        tpe should equal (actualTpe)
                        leaf should equal (internalData(1).get(count, actualTpe))
                    }
                    override def visitMissing()(implicit tpe: DataType): Unit = {
                        count -= 1
                        val actualTpe = schema.fields(count).dataType
                        tpe should equal (actualTpe)
                        internalData(1).get(count, actualTpe) should equal (null)
                    }
                }
                internalData(1).subAcceptAllRev(visitor)(schema)
                count should equal (0)
            }
        }
    }
    
}