package de.unikl.cs.dbis.waves.util

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SchemaFixture

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow

class PathKeySpec extends WavesSpec 
    with SchemaFixture {

    "A PathKey" when {
        "being created" should {
            "never be empty" in {
                an [AssertionError] should be thrownBy (new PathKey(Seq.empty))
            }
        }
        "it is not nested" should {
            "have that node as head" in {
                PathKey("foo").head should equal ("foo")
            }

            "have no tail" in {
                an [NoSuchElementException] should be thrownBy (PathKey("foo").tail)
            }

            "not be nested" in {
                PathKey("foo") should not be 'nested
            }

            "have a valid string representation" in {
                val name = "foo"
                val key = PathKey(name)
                key.toString should equal (name)
                key.toSpark should equal (name)
                key.toDotfreeString should equal (name)
                key.toCol should equal (col(name))
            }

            "have a maximum definition level of 1" in {
                PathKey("foo").maxDefinitionLevel should equal (1)
            }

            "append successfully" in {
                val foo = PathKey("foo")
                foo :+ "bar" should equal (PathKey("foo.bar"))
                Then("The original PathKey is unchanged")
                foo should equal(PathKey("foo"))
            }

            "prepend successfully" in {
                val foo = PathKey("foo")
                "bar" +: foo should equal (PathKey("bar.foo"))
                Then("The original PathKey is unchanged")
                foo should equal(PathKey("foo"))
            }

            "contain itself" in {
                PathKey("foo") isPrefixOf PathKey("foo") should equal (true)
            }

            "contain sub-paths" in {
                PathKey("foo") isPrefixOf PathKey("foo.bar") should equal (true)
            }

            "not contain other path keys" in {
                val foo = PathKey("foo")
                foo isPrefixOf PathKey("bar") should equal (false)
                foo isPrefixOf PathKey("bar.foo") should equal (false)
            }

            "equal itself" in {
                val foo = PathKey("foo")
                foo.equals(foo) should equal (true)
                foo.equals(PathKey("foo")) should equal (true)
            }

            "not equal different paths" in {
                PathKey("foo").equals(PathKey("bar")) should equal (false) 
                PathKey("foo").equals(PathKey("foo.bar")) should equal (false)
                PathKey("foo").equals(PathKey("bar.foo")) should equal (false)
            }

            "retrieve the data from an internal row" in {
                When("the key exists")
                PathKey("a").retrieveFrom(internalData(0), schema) should equal (Some(5))
                PathKey("a").present(internalData(0), schema) should equal (true)
                When("the key does not exist")
                PathKey("a").retrieveFrom(internalData(4), schema) should equal (None)
                PathKey("a").present(internalData(4), schema) should equal (false)
                When("the key is not part of the schema")
                an [IllegalArgumentException] should be thrownBy (PathKey("foo").retrieveFrom(internalData(0), schema))
            }

            "retrieve the data from a row" in {
                When("the key exists")
                PathKey("a").retrieveFrom(data(0), schema) should equal (Some(5))
                PathKey("a").present(data(0), schema) should equal (true)
                When("the key does not exist")
                PathKey("a").retrieveFrom(data(4), schema) should equal (None)
                PathKey("a").present(data(4), schema) should equal (false)
                When("the key is not part of the schema")
                an [IllegalArgumentException] should be thrownBy (PathKey("foo").retrieveFrom(data(0), schema))
            }

            "retrieve a schema element from a schema" in {
                When("the key exists")
                PathKey("a").retrieveFrom(schema) should equal (Some(IntegerType))
                When("the key is not part of the schema")
                PathKey("foo").retrieveFrom(schema) should equal (None)
            }
        }
        "it is nested" should {
            "have have its first step as head" in {
                PathKey("foo.bar").head should equal ("foo")
            }

            "have the correct tail" in {
                PathKey("foo.bar").tail should equal (PathKey("bar"))
            }

            "be nested" in {
                PathKey("foo.bar") shouldBe 'nested
            }

            "have a valid string representation" in {
                val name = "foo.bar"
                val key = PathKey(name)
                key.toString should equal (name)
                key.toSpark should equal (name)
                key.toDotfreeString should equal ("foo/bar")
                key.toCol should equal (col(name))
            }

            "have the correct maximum definition level" in {
                PathKey("foo.bar").maxDefinitionLevel should equal (2)
            }

            "append successfully" in {
                val foo = PathKey("foo.bar")
                foo :+ "baz" should equal (PathKey("foo.bar.baz"))
                Then("The original PathKey is unchanged")
                foo should equal(PathKey("foo.bar"))
            }

            "prepend successfully" in {
                val foo = PathKey("foo.bar")
                "baz" +: foo should equal (PathKey("baz.foo.bar"))
                Then("The original PathKey is unchanged")
                foo should equal(PathKey("foo.bar"))
            }

            "contain itself" in {
                PathKey("foo.bar") isPrefixOf PathKey("foo.bar") should equal (true)
            }

            "contain sub-paths" in {
                PathKey("foo.bar") isPrefixOf PathKey("foo.bar.baz") should equal (true)
            }

            "not contain other path keys" in {
                val foo = PathKey("foo.bar")
                foo isPrefixOf PathKey("bar") should equal (false)
                foo isPrefixOf PathKey("bar.foo") should equal (false)
                foo isPrefixOf PathKey("foo") should equal (false)
            }

            "equal itself" in {
                val foo = PathKey("foo.bar")
                foo.equals(foo) should equal (true)
                foo.equals(PathKey("foo.bar")) should equal (true)
            }

            "not equal different paths" in {
                PathKey("foo.bar").equals(PathKey("bar")) should equal (false) 
                PathKey("foo.bar").equals(PathKey("foo")) should equal (false)
                PathKey("foo.bar").equals(PathKey("bar.foo")) should equal (false)
            }

            "retrieve the data from an internal row" in {
                When("the key exists")
                PathKey("b.d").retrieveFrom(internalData(0), schema) should equal (Some(5))
                PathKey("b.d").present(internalData(0), schema) should equal (true)
                When("the key does not exist")
                PathKey("b.d").retrieveFrom(internalData(7), schema) should equal (None)
                PathKey("b.d").present(internalData(7), schema) should equal (false)
                PathKey("b.d").retrieveFrom(internalData(5), schema) should equal (None)
                PathKey("b.d").present(internalData(5), schema) should equal (false)
                When("the key is not part of the schema")
                an [IllegalArgumentException] should be thrownBy (PathKey("foo").retrieveFrom(internalData(0), schema))
            }

            "retrieve the data from a row" in {
                When("the key exists")
                PathKey("b.d").retrieveFrom(data(0), schema) should equal (Some(5))
                PathKey("b.d").present(data(0), schema) should equal (true)
                When("the key does not exist")
                PathKey("b.d").retrieveFrom(data(7), schema) should equal (None)
                PathKey("b.d").present(data(7), schema) should equal (false)
                PathKey("b.d").retrieveFrom(data(5), schema) should equal (None)
                PathKey("b.d").present(data(5), schema) should equal (false)
                When("the key is not part of the schema")
                an [IllegalArgumentException] should be thrownBy (PathKey("foo").retrieveFrom(data(0), schema))
            }

            "retrieve a schema element from a schema" in {
                When("the key exists")
                PathKey("b.d").retrieveFrom(schema) should equal (Some(IntegerType))
                When("the key is not part of the schema")
                PathKey("foo").retrieveFrom(schema) should equal (None)
            }
        }
    }
    
}