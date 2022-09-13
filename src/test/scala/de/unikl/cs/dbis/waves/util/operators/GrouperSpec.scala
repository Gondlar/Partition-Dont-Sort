package de.unikl.cs.dbis.waves.util.operators

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrame

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructType

class GrouperSpec extends WavesSpec
  with DataFrame {

  "A Grouper " should {
    "group correctly" in {
      When("grouping the data")
      val groupedDf = TestGrouper(df)

      Then("the resulting df has the correct schema")
      groupedDf.columns should contain theSameElementsAs (Seq(TestGrouper.GROUP_COLUMN.toString, TestGrouper.COUNT_COLUMN.toString))

      And("the resulting df contains the correct data")
      groupedDf.count should equal (2)
      val countIndex = groupedDf.schema.fieldIndex(TestGrouper.COUNT_COLUMN.toString)
      val groupIndex = groupedDf.schema.fieldIndex(TestGrouper.GROUP_COLUMN)
      val grouped = groupedDf.collect()
      grouped.map(_.getLong(countIndex)) should contain theSameElementsAs (Seq(4, 4))
      grouped.map(_.getBoolean(groupIndex)) should contain theSameElementsAs (Seq(true, false))
    }
    "match the groups to the correct rows" in {
      Given("grouped dfs and the original data")
      val groupedDf = TestGrouper(df)
      val buckets = Seq( groupedDf.filter(TestGrouper.GROUP_COLUMN.col === true)
                       , groupedDf.filter(TestGrouper.GROUP_COLUMN.col === false)
                       )

      When("we match the original data frame to the groups")      
      val matchedDf = TestGrouper.matchAll(buckets, df)

      Then("the resulting df has the correct schema")
      matchedDf.columns should contain theSameElementsAs (df.columns :+ TestGrouper.PARTITION_COLUMN.toString)

      And("the resulting df has the correct contents")
      matchedDf.count should equal (8)
      val partitionIndex = matchedDf.schema.fieldIndex(TestGrouper.PARTITION_COLUMN)
      val aIndex = matchedDf.schema.fieldIndex("a")
      matchedDf.drop(TestGrouper.PARTITION_COLUMN.col).collect should contain theSameElementsAs (df.collect)
      forAll(matchedDf.collect) ( row =>
        if (row.isNullAt(aIndex)) {
          row.getInt(partitionIndex) should equal (0)
        } else {
          row.getInt(partitionIndex) should equal (1)
        }
      )
    }
    "order the data by the grouped data" in {
      Given("a sorted grouped df and the original data")
      val groupedDf = TestGrouper(df).orderBy(TestGrouper.GROUP_COLUMN.asc)

      When("we sort the original data frame using the groups")      
      val sortedDf = TestGrouper.sort(groupedDf, df)

      Then("the resulting df has the correct schema")
      sortedDf.schema should equal (df.schema)

      And("the resulting df has the correct contents")
      sortedDf.count should equal (8)
      val aIndex = sortedDf.schema.fieldIndex("a")
      val sortedRows = sortedDf.collect()
      sortedRows should contain theSameElementsAs (df.collect())
      sortedRows.map(_.isNullAt(aIndex)) shouldBe sorted
    }
    "come up with the correct count" in {
      TestGrouper.count(TestGrouper(df)) should equal (8)
    }
  }

  object TestGrouper extends Grouper(TempColumn("test")) {
    override def apply(schema: StructType): Column = col("a").isNull.as(GROUP_COLUMN)
  }
}