package de.unikl.cs.dbis.waves.testjobs.query

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec

import scala.collection.JavaConverters._

class DeletedTweetsPerUserSpec extends WavesSpec
  with QueryFixture {

  "The DeletedTweetsPerUser Query job" when {
    "using waves" should {
      behave like queryWithResult("4082", true, {
        DeletedTweetsPerUser.main(args)
      })
    }
    "using parquet" should {
      behave like queryWithResult("4082", false, {
        DeletedTweetsPerUser.main(args :+ "useWaves=false")
      })
    }
  }
}
