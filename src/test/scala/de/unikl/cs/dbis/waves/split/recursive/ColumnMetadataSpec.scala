package de.unikl.cs.dbis.waves.split.recursive

import de.unikl.cs.dbis.waves.WavesSpec

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

class ColumnMetadataSpec extends WavesSpec {

  "The ColumnMetadata" should {
    "have a gini coefficient as if it was uniformly districuted" in {
      // Test on int metadata, method is defined in base calss
      val meta = IntColumnMetadata(0, 10, 4)
      meta.gini should equal (0.75)
    }
    "be constructable from a row" when {
      "it contains Booleans" in {
        val row = makeRowOfType(BooleanType, false, true, 2)
        ColumnMetadata.fromRows(row, 0, 1, 2).value should equal (BooleanColumnMetadata(2))
      }
      "it contains integers" in {
        val row = makeRowOfType(IntegerType, 0, 10, 5)
        ColumnMetadata.fromRows(row, 0, 1, 2).value should equal (IntColumnMetadata(0, 10, 5))
      }
      "it contains longs" in {
        val row = makeRowOfType(LongType, 0l, 10l, 5)
        ColumnMetadata.fromRows(row, 0, 1, 2).value should equal (LongColumnMetadata(0, 10, 5))
      }
      "it contains doubles" in {
        val row = makeRowOfType(DoubleType, 0d, 10d, 5)
        ColumnMetadata.fromRows(row, 0, 1, 2).value should equal (DoubleColumnMetadata(0, 10, 5))
      }
      "it contains Strings" in {
        val row = makeRowOfType(StringType, "abc", "def", 5)
        ColumnMetadata.fromRows(row, 0, 1, 2).value should equal (StringColumnMetadata("abc", "def", 5))
      }
    }
    "not be constructable from Arrays" in {
      val row = makeRowOfType(ArrayType(IntegerType), null, null, 5)
      ColumnMetadata.fromRows(row, 0, 1, 2) should not be 'defined
    }
  }
  "Boolean ColumnMetadata" should {
    "construct correctly" in {
      BooleanColumnMetadata(2) should have (
        'min (false),
        'max (true),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (BooleanColumnMetadata(0))
      an [AssertionError] shouldBe thrownBy (BooleanColumnMetadata(3))
    }
    "calculate the correct separator" in {
      BooleanColumnMetadata(2).separator(.75) should equal (false)
    }
    "not be splittable" when {
      "there is just one value" in {
        BooleanColumnMetadata(1).split() should be ('left)
      }
    }
    "split correctly" in {
      BooleanColumnMetadata(2).split(.8).value should equal (
        BooleanColumnMetadata(1), BooleanColumnMetadata(1)
      )
    }
  }
  "Integer ColumnMetadata" should {
    "construct correctly" in {
      IntColumnMetadata(3, 7, 2) should have (
        'min (3),
        'max (7),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (IntColumnMetadata(7, 2, 1))
      an [AssertionError] shouldBe thrownBy (IntColumnMetadata(2, 7, 0))
    }
    "calculate the correct separator" in {
      IntColumnMetadata(0, 19, 10).separator(.75) should equal (14)
    }
    "not be splittable" when {
      "there is just one value" in {
        IntColumnMetadata(0, 19, 1).split() should be ('left)
      }
      "minimum and maximum are equal" in {
        IntColumnMetadata(10, 10, 2).split() should be ('left)
      }
    }
    "split correctly" in {
      IntColumnMetadata(5, 14, 2).split(.8).value should equal (
        IntColumnMetadata(5, 12, 1), IntColumnMetadata(13, 14, 1)
      )
    }
  }
  "Long ColumnMetadata" should {
    "construct correctly" in {
      LongColumnMetadata(3, 7, 2) should have (
        'min (3),
        'max (7),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (LongColumnMetadata(7, 2, 1))
      an [AssertionError] shouldBe thrownBy (LongColumnMetadata(2, 7, 0))
    }
    "calculate the correct separator" in {
      LongColumnMetadata(0, 19, 10).separator(.75) should equal (14)
    }
    "not be splittable" when {
      "there is just one value" in {
        LongColumnMetadata(0, 19, 1).split() should be ('left)
      }
      "minimum and maximum are equal" in {
        LongColumnMetadata(10, 10, 2).split() should be ('left)
      }
    }
    "split correctly" in {
      LongColumnMetadata(5, 14, 2).split(.8).value should equal (
        LongColumnMetadata(5, 12, 1), LongColumnMetadata(13, 14, 1)
      )
    }
  }
  "Double ColumnMetadata" should {
    "construct correctly" in {
      DoubleColumnMetadata(3, 7, 2) should have (
        'min (3),
        'max (7),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (DoubleColumnMetadata(7, 2, 1))
      an [AssertionError] shouldBe thrownBy (DoubleColumnMetadata(2, 7, 0))
    }
    "calculate the correct separator" in {
      DoubleColumnMetadata(0, 20, 10).separator(.75) should equal (15)
    }
    "not be splittable" when {
      "there is just one value" in {
        DoubleColumnMetadata(0, 19, 1).split() should be ('left)
      }
      "minimum and maximum are equal" in {
        DoubleColumnMetadata(10, 10, 2).split() should be ('left)
      }
    }
    "split correctly" in {
      DoubleColumnMetadata(5, 15, 2).split(.8).value should equal (
        DoubleColumnMetadata(5, 13, 1), DoubleColumnMetadata(13, 15, 1)
      )
    }
  }
  "String ColumnMetadata" should {
    "construct correctly" in {
      StringColumnMetadata("abc", "def", 2) should have (
        'min ("abc"),
        'max ("def"),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (StringColumnMetadata("def", "abc", 1))
      an [AssertionError] shouldBe thrownBy (StringColumnMetadata("abc", "def", 0))
    }
    "calculate the correct separator" when {
      "the strings do not contain unicode" in {
        StringColumnMetadata("#abc", "#def", 10).separator(.75) should equal ("#cde")
        StringColumnMetadata("#abc", "#cba", 10).separator() should equal ("#bb聡")
      }
      "the strings contain unicode" in {
        val separator = StringColumnMetadata("A", "🐬", 10).separator()
        separator shouldBe (<=("🐬"))
        separator shouldBe (>=("A"))
        separator should equal ("氿渖")
      }
      "the min boundary is empty" in {
        // since the emtpy string is the smallest one, ist must be min
        // if min and max are both empty, the column is not splittable
        StringColumnMetadata("", "Z", 2).separator() should equal ("-")
      }
      "there is a carry in the calculation" in {
        StringColumnMetadata(s"A${(Char.MaxValue-10).toChar}", "BZ", 2).separator() should equal ("B'")
      }
    }
    "not be splittable" when {
      "there is just one value" in {
        StringColumnMetadata("abc", "cde", 1).split() should be ('left)
      }
      "minimum and maximum are equal" in {
        StringColumnMetadata("abc", "abc", 2).split() should be ('left)
      }
    }
    "split correctly" when {
      "the max string is larger at every char" in {
        StringColumnMetadata("#abc", "#def", 8).split(.75).value should equal (
          StringColumnMetadata("#abc", "#cde", 6), StringColumnMetadata("#cdf", "#def", 2)
        )
      }
      "the max string has a character that is the successor of the corresponding min character" in {
        StringColumnMetadata("#abccc", "#bac", 2).split().value should equal (
          StringColumnMetadata("#abccc", "#a聡c耱", 1), StringColumnMetadata("#a聡c耲", "#bac", 1)
        )
      }
    }
  }

  def makeRowOfType(tpe: DataType, min: Any, max: Any, distinct: Long) = {
    val schema = StructType(Array(StructField("min", tpe), StructField("max", tpe), StructField("distinct", LongType)))
    new GenericRowWithSchema(Array[Any](min, max, distinct), schema)
  }
}