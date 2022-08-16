package de.unikl.cs.dbis.waves.util

import de.unikl.cs.dbis.waves.WavesSpec
import org.scalatest.BeforeAndAfterAll

class LoggerSpec extends WavesSpec with BeforeAndAfterAll {

    // Clear the log before this test unit or we get all log events
    // from other tests
    override protected def beforeAll(): Unit = Logger.clear()

    "A Logger" should {
        "be empty when created" in {
            Logger.toString() should equal ("")
        }
        "contain logged events" in {
            When("logging")
            Logger.log("test1", "foo")
            Logger.log("test2", "bar")

            Then("the logger should contain these events")
            val events = Logger.toString().split("\n")
            events should have length (2)

            val first = events(0).split(",")
            first should have length (3)
            noException shouldBe thrownBy (first(0).toLong)
            first(1) should equal ("'test1'")
            first(2) should equal ("'foo'")

            val second = events(1).split(",")
            second should have length (3)
            noException shouldBe thrownBy (second(0).toLong)
            second(1) should equal ("'test2'")
            second(2) should equal ("'bar'")

            And("they are removed by clearing the log")
            Logger.clear()
            Logger.toString() should equal ("")
        }
    }   
}