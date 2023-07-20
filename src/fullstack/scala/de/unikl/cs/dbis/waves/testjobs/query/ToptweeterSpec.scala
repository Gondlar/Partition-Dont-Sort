package de.unikl.cs.dbis.waves.testjobs.query

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec

import scala.collection.JavaConverters._

class ToptweeterSpec extends WavesSpec
  with QueryFixture {

  "The Toptweeter Query job" when {
    "using waves" should {
      behave like queryWithResult("14", true, {
        Toptweeter.main(args)
      }, Seq("'build-scan'", "'chose-buckets'", "'scan-built'"))
    }
    "using parquet" should {
      behave like queryWithResult("14", false, {
        Toptweeter.main(args :+ "useWaves=false")
      })
    }
  }
}
