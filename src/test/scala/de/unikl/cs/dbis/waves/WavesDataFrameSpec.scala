package de.unikl.cs.dbis.waves.util

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.RelationFixture

import de.unikl.cs.dbis.waves.WavesTable.implicits

class WavesDataFrameSpec extends WavesSpec with RelationFixture {

  "A DataFrame" when {
    "it reads directly from a WavesTable" should {
      "retrieve that table" in {
          val df = spark.read.format("de.unikl.cs.dbis.waves").load(directory)
          df.isWavesTable should be (true)
          df.getWavesTable shouldBe defined
          df.getWavesTable.get.name should equal (s"waves $directory")
      }
    }
    "it indirectly reads from a WavesTable" should {
      "not retrieve a table" in {
        val df = spark.read.format("de.unikl.cs.dbis.waves").load(directory).select("a")
        df.isWavesTable should be (false)
        df.getWavesTable shouldBe empty
      }
    }
    "it doesn't read from a  WavesTable" should {
      "not retrieve a table" in {
        df.isWavesTable should be (false)
        df.getWavesTable shouldBe empty
      }
    }
  }   
}