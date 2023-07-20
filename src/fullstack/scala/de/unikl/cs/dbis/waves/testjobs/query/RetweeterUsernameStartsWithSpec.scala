package de.unikl.cs.dbis.waves.testjobs.query

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec

import scala.collection.JavaConverters._

class RetweeterUsernameStartsWithSpec extends WavesSpec
  with QueryFixture {

  "The RetweeterUsernameStartsWith Query job" when {
    "using waves" should {
      behave like queryWithResult("0", true, {
        RetweeterUsernameStartsWith.main(args)
      })
    }
    "using parquet" should {
      behave like queryWithResult("0", false, {
        RetweeterUsernameStartsWith.main(args :+ "useWaves=false")
      })
    }
  }
}
