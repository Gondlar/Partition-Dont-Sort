package de.unikl.cs.dbis.waves.util

import de.unikl.cs.dbis.waves.WavesSpec

class BooleanColumnMetadataSpec extends WavesSpec {

  "The UniformColumnMetadata" should {
    "not be constructable with non-probability values" in {
      an [AssertionError] shouldBe thrownBy (BooleanColumnMetadata(-1))
      an [AssertionError] shouldBe thrownBy (BooleanColumnMetadata(2))
    }
    "have the correct amount of distinct values" when {
      "the colum is all true" in {
        BooleanColumnMetadata.allTrue.distinct should equal (1)
      }
      "the column is all false" in {
        BooleanColumnMetadata.allFalse.distinct should equal (1)
      }
      "the column has a mix of values" in {
        BooleanColumnMetadata.uniform.distinct should equal (2)
      }
    }
    "have the correct gini coefficient" when {
      "the colum is all true" in {
        BooleanColumnMetadata.allTrue.gini should equal (0)
      }
      "the column is all false" in {
        BooleanColumnMetadata.allFalse.gini should equal (0)
      }
      "the column has a mix of values" in {
        BooleanColumnMetadata(.25).gini should equal (.375)
      }
    }
    "have the correct separator" in {
      BooleanColumnMetadata.uniform.separator(.25) should equal (BooleanColumn(false))
    }
    "calculate the correct probability" when {
      "the separator is of the wrong type" in {
        an [IllegalArgumentException] shouldBe thrownBy (BooleanColumnMetadata.uniform.probability(5))
      }
      "the separator is true" in {
        BooleanColumnMetadata(.25).probability(false).value should equal (.75)
      }
      "the separator is false" in {
        BooleanColumnMetadata(.25).probability(true).value should equal (1)
      }
    }
    "calculate the correct split" when {
      "the colum is all true" in {
        BooleanColumnMetadata.allTrue.split(.5) shouldBe ('left)
      }
      "the column is all false" in {
        BooleanColumnMetadata.allFalse.split(.5) shouldBe ('left)
      }
      "the column has a mix of values" in {
        BooleanColumnMetadata(.25).split(.5).value should equal ((BooleanColumnMetadata.allFalse, BooleanColumnMetadata.allTrue))
      }
    }
    "be constructable from counts" when {
      "both counts are correct" in {
        BooleanColumnMetadata.fromCounts(5, 15).value should equal (BooleanColumnMetadata(.75))
      }
      "the false count is invalid" in {
        an [AssertionError] shouldBe thrownBy (BooleanColumnMetadata.fromCounts(-5, 15))
      }
      "the true count is invalid" in {
        an [AssertionError] shouldBe thrownBy (BooleanColumnMetadata.fromCounts(15, -5))
      }
      "the value is never present" in {
        val row = Array(0L, 0L)
        BooleanColumnMetadata.fromCounts(0, 0) shouldBe empty
      }
    }
  }
}