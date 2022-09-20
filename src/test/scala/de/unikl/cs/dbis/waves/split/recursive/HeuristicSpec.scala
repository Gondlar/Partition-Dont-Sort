package de.unikl.cs.dbis.waves.split.recursive

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.util.PathKey

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.DataFrameFixture

class HeuristicSpec extends WavesSpec
  with DataFrameFixture {

  var testHeuristic: TestHeuristic = null

  override protected def beforeEach() = {
    super.beforeEach()
    testHeuristic = TestHeuristic()
  }

  "A Heuristic" when {
    "filtering known paths" should {
      "filter known absent paths" in {
          testHeuristic.filterKnownPaths(Seq(PathKey("foo")), Seq.empty, PathKey("foo.bar")) should be (false)
          testHeuristic.filterKnownPaths(Seq(PathKey("foo")), Seq.empty, PathKey("foo")) should be (false)
      }
      "filter known present paths" in {
          testHeuristic.filterKnownPaths(Seq.empty, Seq(PathKey("foo")), PathKey("foo")) should be (false)
      }
      "keep other paths" in {
          testHeuristic.filterKnownPaths(Seq.empty, Seq(PathKey("foo")), PathKey("foo.bar")) should be (true)
          testHeuristic.filterKnownPaths(Seq.empty, Seq(PathKey("foo")), PathKey("bar")) should be (true)
          testHeuristic.filterKnownPaths(Seq(PathKey("foo")), Seq.empty, PathKey("bar")) should be (true)
      } 
    }
    "calculating" should {
      "find the correct metrics for all Rows" in {
        val res = testHeuristic.allowablePaths(RowwiseCalculator(), df, Seq.empty, Seq.empty, 0)
        res.toSeq should equal (Seq(
            (PathKey("a"),   0, 0),
            (PathKey("b"),   0, 4),
            (PathKey("b.d"), 2, 2)
        ))
      }

      "skip known absent subtrees" in {
        val res = testHeuristic.allowablePaths(RowwiseCalculator(), df, Seq(PathKey("b")), Seq.empty, 0)
        res.toSeq should equal (Seq(
            (PathKey("a"),   0, 0)
        ))
      }

      "skip known present paths" in {
        val res = testHeuristic.allowablePaths(RowwiseCalculator(), df, Seq.empty, Seq(PathKey("b")), 0)
        res.toSeq should equal (Seq(
            (PathKey("a"),   0, 0),
            (PathKey("b.d"), 2, 2)
        ))
      }

      "skip paths outside threshold" in {
        val res = testHeuristic.allowablePaths(RowwiseCalculator(), df, Seq.empty, Seq.empty, 3/8d)
        res.toSeq should equal (Seq(
            (PathKey("a"),   0, 0),
            (PathKey("b"),   0, 4)
        ))
      }
    }
  }
  "The EvenHeuristic" should {
    "find the document with the lowest value" in {
        val res = EvenHeuristic.choose(RowwiseCalculator(), df, Seq.empty, Seq.empty, 0)
        res should (equal (Some(PathKey("a"))) or equal (Some(PathKey("b"))) )
    }
    "find no document when no paths are allowable" in {
        val res = EvenHeuristic.choose(RowwiseCalculator(), emptyDf, Seq.empty, Seq.empty, 0)
        res should equal (None)
    }
  }
  "The SwitchHeuristic" should {
    "find the document with the lowest value" in {
        val res = SwitchHeuristic.choose(RowwiseCalculator(), df, Seq.empty, Seq.empty, 0)
        res should (equal (Some(PathKey("a"))) or equal (Some(PathKey("b"))) )
    }
    "find no document when no paths are allowable" in {
        val res = SwitchHeuristic.choose(RowwiseCalculator(), emptyDf, Seq.empty, Seq.empty, 0)
        res should equal (None)
    }
  }
}

case class TestHeuristic() extends AbstractHeuristic {

  override protected def heuristic(col: ColumnMetric): Int = ???

  override def allowablePaths(
    metric: PartitionMetricCalculator,
    df: DataFrame,
    knownAbsent: Seq[PathKey],
    knownPresent: Seq[PathKey],
    thresh : Double
  ): Iterator[ColumnMetric] = super.allowablePaths(metric, df, knownAbsent, knownPresent, thresh)

  override def filterKnownPaths(
    knownAbsent: Seq[PathKey],
    knownPresent: Seq[PathKey],
    path: PathKey
  ): Boolean = super.filterKnownPaths(knownAbsent, knownPresent, path)
}