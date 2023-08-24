package de.unikl.cs.dbis.waves.util

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SchemaFixture
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow

class OptionalPathKeySpec extends WavesSpec 
    with SchemaFixture {

    "An optional PathKey" when {
        "empty" should {
            "have no head" in {
                an [NoSuchElementException] should be thrownBy (Option.empty[PathKey].head)
            }

            "have no tail" in {
                an [NoSuchElementException] should be thrownBy (Option.empty[PathKey].tail)
            }

            "not be nested" in {
                Option.empty[PathKey].isNested should equal (false)
            }

            "have a valid string representation" in {
                Option.empty[PathKey].toSpark should equal ("*")
            }

            "have a maximum definition level of 0" in {
                Option.empty[PathKey].maxDefinitionLevel should equal (0)
            }

            "append successfully" in {
                val foo = Option.empty[PathKey]
                foo :+ "bar" should equal (Some(PathKey("bar")))
                Then("The original PathKey is unchanged")
                foo should equal(None)
            }

            "prepend successfully" in {
                val foo = Option.empty[PathKey]
                "bar" +: foo should equal (Some(PathKey("bar")))
                Then("The original PathKey is unchanged")
                foo should equal(None)
            }

            "contain itself" in {
                Option.empty[PathKey] isPrefixOf Option.empty[PathKey] should equal (true)
            }

            "contain no non-empty paths" in {
                val foo = Option.empty[PathKey]
                foo.contains(PathKey("bar")) should equal (false)
                foo.contains(Some(PathKey("bar"))) should equal (false)
            }

            "equal itself" in {
                val foo = Option.empty[PathKey]
                foo.equals(foo) should equal (true)
                foo.equals(Option.empty[PathKey]) should equal (true)
            }

            "not equal different paths" in {
                Option.empty[PathKey].equals(Some(PathKey("bar"))) should equal (false) 
                Option.empty[PathKey].equals(PathKey("bar")) should equal (false)
            }

            "retrieve the row itself" in {
                Option.empty[PathKey].retrieveFrom(internalData(0), schema) should equal (Some(internalData(0)))
                Option.empty[PathKey].present(internalData(0), schema) should equal (true)
            }

            "retrieve a schema element from a schema" in {
                Option.empty[PathKey].retrieveFrom(schema) should equal (Some(schema))
            }
        }
        "it has steps" should {
            "have have its first step as head" in {
                Some(PathKey("foo")).head should equal ("foo")
            }

            "have the correct tail" in {
                Some(PathKey("foo")).tail should equal (None)
                Some(PathKey("foo.bar")).tail should equal (Some(PathKey("bar")))
            }

            "be nested in the correct cases" in {
                Some(PathKey("foo")).isNested should equal (false)
                Some(PathKey("foo.bar")).isNested should equal (true)
            }

            "have a valid string representation" in {
                Some(PathKey("foo.bar")).toSpark should equal ("foo.bar")
            }

            "have the correct maximum definition level" in {
                Some(PathKey("foo.bar")).maxDefinitionLevel should equal (2)
            }

            "append successfully" in {
                val foo = Some(PathKey("foo"))
                foo :+ "baz" should equal (Some(PathKey("foo.baz")))
                Then("The original PathKey is unchanged")
                foo should equal(Some(PathKey("foo")))
            }

            "prepend successfully" in {
                val foo = Some(PathKey("foo"))
                "baz" +: foo should equal (Some(PathKey("baz.foo")))
                Then("The original PathKey is unchanged")
                foo should equal(Some(PathKey("foo")))
            }

            "contain itself" in {
                Some(PathKey("foo")) isPrefixOf Some(PathKey("foo")) should equal (true)
            }

            "not contain empty paths" in {
                Some(PathKey("foo")) isPrefixOf None should equal (false)
            }

            "equal itself" in {
                val foo = Some(PathKey("foo"))
                foo.equals(foo) should equal (true)
                foo.equals(Some(PathKey("foo"))) should equal (true)
            }

            "not equal different paths" in {
                Some(PathKey("foo")).equals(PathKey("bar")) should equal (false)
                Some(PathKey("foo")).equals(Some(PathKey("bar"))) should equal (false)
            }

            "retrieve the data from a row" in {
                Some(PathKey("b.d")).retrieveFrom(internalData(0), schema) should equal (Some(5))
                Some(PathKey("b.d")).present(internalData(0), schema) should equal (true)
            }

            "retrieve a schema element from a schema" in {
                Some(PathKey("b.d")).retrieveFrom(schema) should equal (Some(IntegerType))
            }
        }
    }
    
}