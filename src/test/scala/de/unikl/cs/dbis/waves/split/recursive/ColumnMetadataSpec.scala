package de.unikl.cs.dbis.waves.split.recursive

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.util.ColumnValue
import de.unikl.cs.dbis.waves.util.BooleanColumn
import de.unikl.cs.dbis.waves.util.IntegerColumn
import de.unikl.cs.dbis.waves.util.LongColumn
import de.unikl.cs.dbis.waves.util.DoubleColumn
import de.unikl.cs.dbis.waves.util.StringColumn

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

class ColumnMetadataSpec extends WavesSpec {

  "The ColumnMetadata" should {
    "have a gini coefficient as if it was uniformly districuted" in {
      // simply test on int metadata, value is independant of type
      val meta = ColumnMetadata(0, 10, 4)
      meta.gini should equal (0.75)
    }
    "be constructable from a row" when {
      "it contains Booleans" in {
        val row = makeRowOfType(BooleanType, false, true, 2)
        ColumnMetadata.fromRows(row, 0, 1, 2).value should equal (ColumnMetadata(false, true, 2))
      }
      "it contains integers" in {
        val row = makeRowOfType(IntegerType, 0, 10, 5)
        ColumnMetadata.fromRows(row, 0, 1, 2).value should equal (ColumnMetadata(0, 10, 5))
      }
      "it contains longs" in {
        val row = makeRowOfType(LongType, 0l, 10l, 5)
        ColumnMetadata.fromRows(row, 0, 1, 2).value should equal (ColumnMetadata(0L, 10L, 5))
      }
      "it contains doubles" in {
        val row = makeRowOfType(DoubleType, 0d, 10d, 5)
        ColumnMetadata.fromRows(row, 0, 1, 2).value should equal (ColumnMetadata(0d, 10d, 5))
      }
      "it contains Strings" in {
        val row = makeRowOfType(StringType, "abc", "def", 5)
        ColumnMetadata.fromRows(row, 0, 1, 2).value should equal (ColumnMetadata("abc", "def", 5))
      }
    }
    "not be constructable from Arrays" in {
      val row = makeRowOfType(ArrayType(IntegerType), null, null, 5)
      ColumnMetadata.fromRows(row, 0, 1, 2) should not be 'defined
    }
  }
  "Boolean ColumnMetadata" should {
    "construct correctly" in {
      ColumnMetadata(false, true, 2) should have (
        'min (BooleanColumn(false)),
        'max (BooleanColumn(true)),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (ColumnMetadata(true, false, 2))
    }
    "calculate the correct separator" in {
      ColumnMetadata(false, true, 2).separator(.75) should equal (BooleanColumn(false))
    }
    "not be splittable" when {
      "there is just one value" in {
        ColumnMetadata(false, false, 1).split() should be ('left)
      }
    }
    "split correctly" in {
      ColumnMetadata(false, true, 2).split(.8).value should equal (
        ColumnMetadata(false, false, 1), ColumnMetadata(true, true, 1)
      )
    }
  }
  "Integer ColumnMetadata" should {
    "construct correctly" in {
      ColumnMetadata(3, 7, 2) should have (
        'min (IntegerColumn(3)),
        'max (IntegerColumn(7)),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (ColumnMetadata(7, 2, 1))
      an [AssertionError] shouldBe thrownBy (ColumnMetadata(2, 7, 0))
    }
    "calculate the correct separator" in {
      ColumnMetadata(0, 19, 10).separator(.75) should equal (IntegerColumn(14))
    }
    "not be splittable" when {
      "there is just one value" in {
        ColumnMetadata(0, 19, 1).split() should be ('left)
      }
      "minimum and maximum are equal" in {
        ColumnMetadata(10, 10, 2).split() should be ('left)
      }
    }
    "split correctly" in {
      ColumnMetadata(5, 14, 2).split(.8).value should equal (
        ColumnMetadata(5, 12, 1), ColumnMetadata(13, 14, 1)
      )
    }
  }
  "Long ColumnMetadata" should {
    "construct correctly" in {
      ColumnMetadata(3L, 7L, 2) should have (
        'min (LongColumn(3)),
        'max (LongColumn(7)),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (ColumnMetadata(7L, 2L, 1))
      an [AssertionError] shouldBe thrownBy (ColumnMetadata(2L, 7L, 0))
    }
    "calculate the correct separator" in {
      ColumnMetadata(0L, 19L, 10).separator(.75) should equal (LongColumn(14))
    }
    "not be splittable" when {
      "there is just one value" in {
        ColumnMetadata(0L, 19L, 1).split() should be ('left)
      }
      "minimum and maximum are equal" in {
        ColumnMetadata(10L, 10L, 2).split() should be ('left)
      }
    }
    "split correctly" in {
      ColumnMetadata(5L, 14L, 2).split(.8).value should equal (
        ColumnMetadata(5L, 12L, 1), ColumnMetadata(13L, 14L, 1)
      )
    }
  }
  "Double ColumnMetadata" should {
    "construct correctly" in {
      ColumnMetadata(3d, 7d, 2) should have (
        'min (DoubleColumn(3)),
        'max (DoubleColumn(7)),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (ColumnMetadata(7d, 2d, 1))
      an [AssertionError] shouldBe thrownBy (ColumnMetadata(2d, 7d, 0))
    }
    "calculate the correct separator" in {
      ColumnMetadata(0d, 20d, 10).separator(.75) should equal (DoubleColumn(15))
    }
    "not be splittable" when {
      "there is just one value" in {
        ColumnMetadata(0d, 19d, 1).split() should be ('left)
      }
      "minimum and maximum are equal" in {
        ColumnMetadata(10d, 10d, 2).split() should be ('left)
      }
    }
    "split correctly" in {
      ColumnMetadata(5d, 15d, 2).split(.8).value should equal (
        ColumnMetadata(5d, 13d, 1), ColumnMetadata(13d, 15d, 1)
      )
    }
  }
  "String ColumnMetadata" should {
    "construct correctly" in {
      ColumnMetadata("abc", "def", 2) should have (
        'min (StringColumn("abc")),
        'max (StringColumn("def")),
        'distinct (2)
      )
    }
    "not construct if max < min" in {
      an [AssertionError] shouldBe thrownBy (ColumnMetadata("def", "abc", 1))
      an [AssertionError] shouldBe thrownBy (ColumnMetadata("abc", "def", 0))
    }
    "calculate the correct separator" when {
      "the strings do not contain unicode" in {
        ColumnMetadata("#abc", "#def", 10).separator(.75) should equal (StringColumn("#cde"))
        ColumnMetadata("#abc", "#cba", 10).separator() should equal (StringColumn("#bb聡"))
      }
      "the strings contain unicode" in {
        val separator = ColumnMetadata("A", "🐬", 10).separator().asInstanceOf[StringColumn].v
        separator shouldBe (<=("🐬"))
        separator shouldBe (>=("A"))
        separator should equal ("氿渖")
      }
      "the min boundary is empty" in {
        // since the emtpy string is the smallest one, ist must be min
        // if min and max are both empty, the column is not splittable
        ColumnMetadata("", "Z", 2).separator() should equal (StringColumn("-"))
      }
      "there is a carry in the calculation" in {
        ColumnMetadata(s"A${(Char.MaxValue-10).toChar}", "BZ", 2).separator() should equal (StringColumn("B'"))
      }
    }
    "not be splittable" when {
      "there is just one value" in {
        ColumnMetadata("abc", "cde", 1).split() should be ('left)
      }
      "minimum and maximum are equal" in {
        ColumnMetadata("abc", "abc", 2).split() should be ('left)
      }
    }
    "split correctly" when {
      "the max string is larger at every char" in {
        ColumnMetadata("#abc", "#def", 8).split(.75).value should equal (
          ColumnMetadata("#abc", "#cde", 6), ColumnMetadata("#cdf", "#def", 2)
        )
      }
      "the max string has a character that is the successor of the corresponding min character" in {
        ColumnMetadata("#abccc", "#bac", 2).split().value should equal (
          ColumnMetadata("#abccc", "#a聡c耱", 1), ColumnMetadata("#a聡c耲", "#bac", 1)
        )
      }
    }
  }

  def makeRowOfType(tpe: DataType, min: Any, max: Any, distinct: Long) = {
    val schema = StructType(Array(StructField("min", tpe), StructField("max", tpe), StructField("distinct", LongType)))
    new GenericRowWithSchema(Array[Any](min, max, distinct), schema)
  }
}