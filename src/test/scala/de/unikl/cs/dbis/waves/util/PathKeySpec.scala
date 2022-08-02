package de.unikl.cs.dbis.waves.util

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.Schema
import org.apache.spark.sql.types.IntegerType

class PathKeySpec extends WavesSpec 
    with Schema {

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
                an [AssertionError] should be thrownBy (PathKey("foo").tail)
            }

            "not be nested" in {
                PathKey("foo") should not be 'nested
            }

            "have a valid string representation" in {
                PathKey("foo").toString should equal ("foo")
            }

            "have a maximum definition level of 1" in {
                PathKey("foo").maxDefinitionLevel should equal (1)
            }

            "prepend successfully" in {
                val foo = PathKey("foo")
                foo.prepend("bar") should equal (PathKey("bar.foo"))
                Then("The original PathKey is unchanged")
                foo should equal(PathKey("foo"))
            }

            "contain itself" in {
                PathKey("foo").contains(PathKey("foo")) should equal (true)
            }

            "contain sub-paths" in {
                PathKey("foo").contains(PathKey("foo.bar")) should equal (true)
            }

            "not contain other path keys" in {
                val foo = PathKey("foo")
                foo.contains(PathKey("bar")) should equal (false)
                foo.contains(PathKey("bar.foo")) should equal (false)
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

            "retrieve the data from a row" in {
                When("the key exists")
                PathKey("a").retrieveFrom(data(0)) should equal (Right(5))
                When("the key does not exist")
                PathKey("a").retrieveFrom(data(4)) should equal (Left(0))
                When("the key is not part of the schema")
                an [IllegalArgumentException] should be thrownBy (PathKey("foo").retrieveFrom(data(0)))
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
                PathKey("foo.bar").toString should equal ("foo.bar")
            }

            "have the correct maximum definition level" in {
                PathKey("foo.bar").maxDefinitionLevel should equal (2)
            }

            "prepend successfully" in {
                val foo = PathKey("foo.bar")
                foo.prepend("baz") should equal (PathKey("baz.foo.bar"))
                Then("The original PathKey is unchanged")
                foo should equal(PathKey("foo.bar"))
            }

            "contain itself" in {
                PathKey("foo.bar").contains(PathKey("foo.bar")) should equal (true)
            }

            "contain sub-paths" in {
                PathKey("foo.bar").contains(PathKey("foo.bar.baz")) should equal (true)
            }

            "not contain other path keys" in {
                val foo = PathKey("foo.bar")
                foo.contains(PathKey("bar")) should equal (false)
                foo.contains(PathKey("bar.foo")) should equal (false)
                foo.contains(PathKey("foo")) should equal (false)
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

            "retrieve the data from a row" in {
                When("the key exists")
                PathKey("b.d").retrieveFrom(data(0)) should equal (Right(5))
                When("the key does not exist")
                PathKey("b.d").retrieveFrom(data(7)) should equal (Left(0))
                PathKey("b.d").retrieveFrom(data(5)) should equal (Left(1))
                When("the key is not part of the schema")
                an [IllegalArgumentException] should be thrownBy (PathKey("foo").retrieveFrom(data(0)))
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