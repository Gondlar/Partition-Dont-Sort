package de.unikl.cs.dbis.waves.util

import de.unikl.cs.dbis.waves.WavesSpec

class UniformColumnMetadataSpec extends WavesSpec {

  "The UniformColumnMetadata" should {
    "have a gini coefficient as if it was uniformly districuted" in {
      // simply test on int metadata, value is independant of type
      val meta = UniformColumnMetadata(0, 10, 4)
      meta.gini should equal (0.75)
    }
  }
  "Boolean UniformColumnMetadata" should {
    "construct correctly" in {
      UniformColumnMetadata(false, true, 2) should have (
        'min (BooleanColumn(false)),
        'max (BooleanColumn(true)),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (UniformColumnMetadata(true, false, 2))
    }
    "calculate the correct separator" in {
      UniformColumnMetadata(false, true, 2).separator(.75) should equal (BooleanColumn(false))
    }
    "not be splittable" when {
      "there is just one value" in {
        UniformColumnMetadata(false, false, 1).split() should be ('left)
      }
    }
    "split correctly" in {
      UniformColumnMetadata(false, true, 2).split(.8).value should equal (
        UniformColumnMetadata(false, false, 1), UniformColumnMetadata(true, true, 1)
      )
    }
  }
  "Integer UniformColumnMetadata" should {
    "construct correctly" in {
      UniformColumnMetadata(3, 7, 2) should have (
        'min (IntegerColumn(3)),
        'max (IntegerColumn(7)),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (UniformColumnMetadata(7, 2, 1))
      an [AssertionError] shouldBe thrownBy (UniformColumnMetadata(2, 7, 0))
    }
    "calculate the correct separator" in {
      UniformColumnMetadata(0, 19, 10).separator(.75) should equal (IntegerColumn(14))
    }
    "not be splittable" when {
      "there is just one value" in {
        UniformColumnMetadata(0, 19, 1).split() should be ('left)
      }
      "minimum and maximum are equal" in {
        UniformColumnMetadata(10, 10, 2).split() should be ('left)
      }
    }
    "split correctly" in {
      UniformColumnMetadata(5, 14, 2).split(.8).value should equal (
        UniformColumnMetadata(5, 12, 1), UniformColumnMetadata(13, 14, 1)
      )
    }
  }
  "Long UniformColumnMetadata" should {
    "construct correctly" in {
      UniformColumnMetadata(3L, 7L, 2) should have (
        'min (LongColumn(3)),
        'max (LongColumn(7)),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (UniformColumnMetadata(7L, 2L, 1))
      an [AssertionError] shouldBe thrownBy (UniformColumnMetadata(2L, 7L, 0))
    }
    "calculate the correct separator" in {
      UniformColumnMetadata(0L, 19L, 10).separator(.75) should equal (LongColumn(14))
    }
    "not be splittable" when {
      "there is just one value" in {
        UniformColumnMetadata(0L, 19L, 1).split() should be ('left)
      }
      "minimum and maximum are equal" in {
        UniformColumnMetadata(10L, 10L, 2).split() should be ('left)
      }
    }
    "split correctly" in {
      UniformColumnMetadata(5L, 14L, 2).split(.8).value should equal (
        UniformColumnMetadata(5L, 12L, 1), UniformColumnMetadata(13L, 14L, 1)
      )
    }
  }
  "Double UniformColumnMetadata" should {
    "construct correctly" in {
      UniformColumnMetadata(3d, 7d, 2) should have (
        'min (DoubleColumn(3)),
        'max (DoubleColumn(7)),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (UniformColumnMetadata(7d, 2d, 1))
      an [AssertionError] shouldBe thrownBy (UniformColumnMetadata(2d, 7d, 0))
    }
    "calculate the correct separator" in {
      UniformColumnMetadata(0d, 20d, 10).separator(.75) should equal (DoubleColumn(15))
    }
    "not be splittable" when {
      "there is just one value" in {
        UniformColumnMetadata(0d, 19d, 1).split() should be ('left)
      }
      "minimum and maximum are equal" in {
        UniformColumnMetadata(10d, 10d, 2).split() should be ('left)
      }
    }
    "split correctly" in {
      UniformColumnMetadata(5d, 15d, 2).split(.8).value should equal (
        UniformColumnMetadata(5d, 13d, 1), UniformColumnMetadata(13d, 15d, 1)
      )
    }
  }
  "String UniformColumnMetadata" should {
    "construct correctly" in {
      UniformColumnMetadata("abc", "def", 2) should have (
        'min (StringColumn("abc")),
        'max (StringColumn("def")),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (UniformColumnMetadata("def", "abc", 1))
      an [AssertionError] shouldBe thrownBy (UniformColumnMetadata("abc", "def", 0))
    }
    "calculate the correct separator" when {
      "the strings do not contain unicode" in {
        UniformColumnMetadata("#abc", "#def", 10).separator(.75) should equal (StringColumn("#cde"))
        UniformColumnMetadata("#abc", "#cba", 10).separator() should equal (StringColumn("#bb聡"))
      }
      "the strings contain unicode" in {
        val separator = UniformColumnMetadata("A", "🐬", 10).separator().asInstanceOf[StringColumn].v
        separator shouldBe (<=("🐬"))
        separator shouldBe (>=("A"))
        separator should equal ("氿渖")
      }
      "the min boundary is empty" in {
        // since the emtpy string is the smallest one, ist must be min
        // if min and max are both empty, the column is not splittable
        UniformColumnMetadata("", "Z", 2).separator() should equal (StringColumn("-"))
      }
      "there is a carry in the calculation" in {
        UniformColumnMetadata(s"A${(Char.MaxValue-10).toChar}", "BZ", 2).separator() should equal (StringColumn("B'"))
      }
    }
    "not be splittable" when {
      "there is just one value" in {
        UniformColumnMetadata("abc", "cde", 1).split() should be ('left)
      }
      "minimum and maximum are equal" in {
        UniformColumnMetadata("abc", "abc", 2).split() should be ('left)
      }
    }
    "split correctly" when {
      "the max string is larger at every char" in {
        UniformColumnMetadata("#abc", "#def", 8).split(.75).value should equal (
          UniformColumnMetadata("#abc", "#cde", 6), UniformColumnMetadata("#cdf", "#def", 2)
        )
      }
      "the max string has a character that is the successor of the corresponding min character" in {
        UniformColumnMetadata("#abccc", "#bac", 2).split().value should equal (
          UniformColumnMetadata("#abccc", "#a聡c耱", 1), UniformColumnMetadata("#a聡c耲", "#bac", 1)
        )
      }
    }
  }

  def makeRowOfType[Type](min: Type, max: Type, distinct: Long)
    = Seq[Any](min, max, distinct)
}