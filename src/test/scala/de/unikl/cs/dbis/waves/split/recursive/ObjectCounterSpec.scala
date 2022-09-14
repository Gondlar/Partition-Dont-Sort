package de.unikl.cs.dbis.waves.split.recursive

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.SchemaFixture
import de.unikl.cs.dbis.waves.util.PathKey

import org.apache.spark.sql.types.StructType

class ObjectCounterSpec extends WavesSpec
    with SchemaFixture with ObjectCounterFixture {

    "An ObjectCounter" when {
        "being created" should {
            "initialize to the correct length" in {
                val test = ObjectCounter(schema)
                test.values should have length (3)
                test.size should equal (3)
            }

            "have all zero counts" in {
                ObjectCounter(3).values should contain only (0)
            }
            "find the correct paths" in {
                When("determining paths")
                val paths = ObjectCounter.paths(schema)
                Then("all paths are correct")
                paths should equal (Seq(PathKey("a"), PathKey("b"), PathKey("b.d")))

                When("Schema is empty")
                val paths2 = ObjectCounter(StructType(Seq()))
                Then("no paths are found")
                paths2.values should have length (0)
            }
        }
        "not empty" should {
            "have the correct size" in {
                lhs.size should equal (3)
            }

            "add correctly" in {
                lhs += rhs
                lhs.values should equal (Array(12, 4, 3))
            }

            "subtract correctly" in {
                lhs -= rhs
                lhs.values should equal (Array(-2, 2, -1))
            }

            "multiply correctly" in {
                lhs *= rhs
                lhs.values should equal (Array(35, 3, 2))
            }

            "add constants correctly" in {
                lhs += 5
                lhs.values should equal (Array(10, 8, 6))
            }

            "subtract constants correctly" in {
                lhs -= 3
                lhs.values should equal (Array(2, 0, -2))
            }

            "multiply constants correctly" in {
                lhs *= 10
                lhs.values should equal (Array(50, 30, 10))
            }

            "map correctly" in {
                val valueCalls = Array(0,0,0)
                When("map is invoked")
                lhs.map(value => value match {
                    case 5 => { valueCalls(0) += 1; 42 }
                    case 3 => { valueCalls(1) += 1; 1337 }
                    case 1 => { valueCalls(2) += 1; 420 }
                    case _ => fail("invalid pair called")
                })

                Then("all values are processed once")
                valueCalls should contain only (1)

                And("all results are written back at the correct position")
                lhs.values should equal (Array(42, 1337, 420))
            }

            "combine correctly" in {
                val valueCalls = Array(0,0,0)
                When("combine is invoked")
                lhs.combine(rhs, (left, right) => (left, right) match {
                    case (5,7) => { valueCalls(0) += 1; 42 }
                    case (3,1) => { valueCalls(1) += 1; 1337 }
                    case (1,2) => { valueCalls(2) += 1; 420 }
                    case _ => fail("invalid pair called")
                })

                Then("all pairs are called once")
                valueCalls should contain only (1)

                And("all results are written back at the correct position")
                lhs.values should equal (Array(42, 1337, 420))
            }

            "fail to combine differently sized partners" in {
                Given("a larger counter")
                val larger = ObjectCounter(5)
                assertThrows[IllegalArgumentException] {
                    lhs.combine(larger, (_, _) => fail("Called f despite different size"))
                }
                Given("a smaller counter")
                val smaller = ObjectCounter(2)
                assertThrows[IllegalArgumentException] {
                    lhs.combine(smaller, (_, _) => fail("Called f despite different size"))
                }
            }
            "equal itself" in {
              lhs should equal (lhs)
            }
            "not equals counters with other values" in {
              lhs should not equal (rhs)
              lhs should not equal (ObjectCounter(0))
            }
        }
        "empty" should {
            "have size 0" in {
                ObjectCounter(0).size should equal (0)
            }
            "remain empty for all mathematical operations" in {
                Given("an empty counter")
                val counter = ObjectCounter(0)

                When("adding")
                counter += counter
                counter.values should have length (0)
                counter.size should equal (0)

                When("subtracting")
                counter -= counter
                counter.values should have length (0)
                counter.size should equal (0)

                When("multiplying")
                counter *= counter
                counter.values should have length (0)
                counter.size should equal (0)

                When("adding a constant")
                counter += 5
                counter.values should have length (0)
                counter.size should equal (0)

                When("subtracting a constant")
                counter -= 5
                counter.values should have length (0)
                counter.size should equal (0)

                When("multiplying a constant")
                counter *= 5
                counter.values should have length (0)
                counter.size should equal (0)
            }

            "do nothing when mapping" in {
                Given("an empty counter")
                val counter = ObjectCounter(0)

                When("mapping")
                counter.map(_ => { fail("invalid call to f") })

                Then("it is still empty")
                counter.values should have length (0)
                counter.size should equal (0)
            }

            "do nothing when combining" in {
                Given("an empty counter")
                val counter = ObjectCounter(0)

                When("mapping")
                counter.combine(counter, (_, _) => { fail("invalid call to f") })

                Then("it is still empty")
                counter.values should have length (0)
                counter.size should equal (0)
            }
            "equal itself" in {
              ObjectCounter(0) should equal (ObjectCounter(0))
            }
            "not equals counters with values" in {
              ObjectCounter(0) should not equal (rhs)
            }
        }
    }
    
}