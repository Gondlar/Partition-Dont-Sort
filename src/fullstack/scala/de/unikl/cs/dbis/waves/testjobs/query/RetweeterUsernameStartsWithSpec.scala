package de.unikl.cs.dbis.waves.testjobs.query

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec

import scala.collection.JavaConverters._

class RetweeterUsernameStartsWithSpec extends WavesSpec
  with QueryFixture {

  "The UsernameStartsWith Query job" when {
    "using waves" should {
      behave like queryWithResult("1", true, {
        RetweeterUsernameStartsWith.main(args)
      })
    }
    "using parquet" should {
      behave like queryWithResult("1", false, {
        RetweeterUsernameStartsWith.main(args :+ "useWaves=false")
      })
    }
  }
}
