package de.unikl.cs.dbis.waves.util.operators

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.StructType

class AbstractGrouperSpec extends WavesSpec
  with DataFrameFixture {

  "An AbstractGrouper " should {
    "group correctly" in {
      When("grouping the data")
      val groupedDf = TestGrouper(df)

      Then("the resulting df has the correct schema")
      groupedDf.columns should contain theSameElementsAs (TestGrouper.columns)

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
      matchedDf.columns should contain theSameElementsAs (df.columns :+ TestGrouper.matchColumn.toString)

      And("the resulting df has the correct contents")
      matchedDf.count should equal (8)
      val partitionIndex = matchedDf.schema.fieldIndex(TestGrouper.matchColumn)
      val aIndex = matchedDf.schema.fieldIndex("a")
      matchedDf.drop(TestGrouper.matchColumn.col).collect should contain theSameElementsAs (df.collect)
      forAll(matchedDf.collect) ( row =>
        if (row.isNullAt(aIndex)) {
          row.getInt(partitionIndex) should equal (0)
        } else {
          row.getInt(partitionIndex) should equal (1)
        }
      )
    }
    "find all data belonging to a bucket" in {
      Given("grouped dfs and the original data")
      val groupedDf = TestGrouper(df).filter(TestGrouper.GROUP_COLUMN.col === true)

      When("we find the original data")
      val foundData = TestGrouper.find(groupedDf, df)

      Then("the resultung df has the correct schema")
      foundData.schema should equal (df.schema)

      And("the resulting df has the correct contents")
      foundData.count should equal (4)
      foundData.collect should contain theSameElementsAs (df.filter(col("a").isNull).collect())
    }
    "order the data by the grouped data" in {
      Given("a sorted grouped df and the original data")
      val groupedDf = TestGrouper(df).orderBy(TestGrouper.GROUP_COLUMN.asc)

      When("we sort the original data frame using the groups")      
      val sortedDf = TestGrouper.sort(groupedDf, df)

      Then("the resulting df has the correct schema")
      sortedDf.schema should equal (df.schema)

      And("the resulting df has only one partition")
      sortedDf.rdd.getNumPartitions should equal (1)

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
    "report the correct column names" in {
      TestGrouper.columns should equal (Seq(TestGrouper.GROUP_COLUMN.toString, "count"))
    }
    "convert to itself without ungrouping" in {
      Given("a grouper and a df grouped by it")
      val grouper = new AbstractGrouper(TempColumn("test")) {
        var testing = false

        override def apply(schema: StructType): Column
          = col("a").isNull.as(GROUP_COLUMN)

        override def find(bucket: DataFrame, data: DataFrame)
          = fail // should never be called

        override def group(data: DataFrame): DataFrame
          // should never be called in the actual test
          = if (testing) fail else super.group(data)
      }
      val groupedDf = grouper(df)
      grouper.testing = true

      When("we convert the data from itself")
      val regrouped = grouper.from(grouper, groupedDf, df)

      Then("the resulting data is identical")
      regrouped.collect() should contain theSameElementsAs (groupedDf.collect())
    }
    "convert to other groupers correctly" in {
      Given("a df grouped by some grouper")
      val grouped = DefinitionLevelGrouper(df)

      When("we regroup the data")
      val regrouped = TestGrouper.from(DefinitionLevelGrouper, grouped, df)

      Then("the resulting df has the correct schema")
      regrouped.columns should contain theSameElementsAs (TestGrouper.columns)

      And("the resulting df contains the correct data")
      regrouped.count should equal (2)
      val countIndex = regrouped.schema.fieldIndex(TestGrouper.COUNT_COLUMN.toString)
      val groupIndex = regrouped.schema.fieldIndex(TestGrouper.GROUP_COLUMN)
      val regroupedData = regrouped.collect()
      regroupedData.map(_.getLong(countIndex)) should contain theSameElementsAs (Seq(4, 4))
      regroupedData.map(_.getBoolean(groupIndex)) should contain theSameElementsAs (Seq(true, false))
    }
  }

  object TestGrouper extends AbstractGrouper(TempColumn("test")) {
    override def apply(schema: StructType): Column = col("a").isNull.as(GROUP_COLUMN)
  }
}