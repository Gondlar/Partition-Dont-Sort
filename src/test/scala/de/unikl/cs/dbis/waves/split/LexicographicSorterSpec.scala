package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import scala.collection.mutable.{ArrayBuilder, WrappedArray}
import de.unikl.cs.dbis.waves.util.operators.{Grouper,DefinitionLevelGrouper}
import de.unikl.cs.dbis.waves.util.operators.PresenceGrouper

class LexicographicSorterSpec extends WavesSpec
  with DataFrameFixture {

  "The LexicographicSorter" should {
    "sort lexicographically" in {
      val data = TestSorter.testSort(df).collect().map(row =>
        row.getSeq[Int](row.fieldIndex(DefinitionLevelGrouper.GROUP_COLUMN))
      )
      data should contain theSameElementsInOrderAs (Seq( Seq(0, 0, 0)
                                                       , Seq(0, 1, 1)
                                                       , Seq(0, 1, 2)
                                                       , Seq(1, 0, 0)
                                                       , Seq(1, 1, 1)
                                                       , Seq(1, 1, 2)
      ))
    }
  }

  object TestSorter extends GroupedSplitter with LexicographicSorter {
    override protected def load(context: Unit): DataFrame = df
    override protected def splitGrouper: Grouper = ???
    override protected def split(df: DataFrame): Seq[DataFrame] = ???
    override protected def buildTree(buckets: Seq[DataFrame]): Unit = ???
    def testSort(df: DataFrame) = sort(sortGrouper(df))
  }
}
