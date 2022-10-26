package de.unikl.cs.dbis.waves.testjobs.query

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec

import scala.collection.JavaConverters._

class UsernameStartsWithSpec extends WavesSpec
  with QueryFixture {

  "The UsernameStartsWith Query job" when {
    "using waves" should {
      behave like queryWithResult("20", true, {
        UsernameStartsWith.main(args)
      })
    }
    "using parquet" should {
      behave like queryWithResult("20", false, {
        UsernameStartsWith.main(args :+ "useWaves=false")
      })
    }
  }
}
