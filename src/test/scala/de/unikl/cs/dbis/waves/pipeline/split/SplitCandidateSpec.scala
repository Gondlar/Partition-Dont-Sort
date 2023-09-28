package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.partitions._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.split.recursive.ColumnMetadata
import de.unikl.cs.dbis.waves.split.recursive.RSIGraph
import de.unikl.cs.dbis.waves.util.PathKey

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame

class SplitCandidateSpec extends WavesSpec
  with DataFrameFixture {

  "The SplitCandidate" can {
    "split a graph" when {
      "it is a presence split" when {
        "it is a valid location" in {
          presenceCandidate.split(graph).value should equal ((
            RSIGraph(("a", 1d, RSIGraph(("b", .5, RSIGraph(leafMetadata = Some(ColumnMetadata(0, 9, 6))))))),
            RSIGraph(("a", 0d, RSIGraph(("b", .5, RSIGraph(leafMetadata = Some(ColumnMetadata(0, 9, 6)))))))
          ))
        }
        "it is an invalid location" in {
          val graph_present = RSIGraph(("a", 1d, RSIGraph(("b", .5, RSIGraph.empty))))
          val graph_absent = RSIGraph(("a", 0d, RSIGraph(("b", .5, RSIGraph.empty))))
          presenceCandidate.split(graph_absent) shouldBe ('left)
          presenceCandidate.split(graph_present) shouldBe ('left)
        }
      }
      "it is a median split" in {
        val (left, right) = medianCandidate.split(graph).value
        left should equal (RSIGraph(("a", 1d, RSIGraph(("b", 1d, RSIGraph(leafMetadata = Some(ColumnMetadata(0, 4, 3))))))))
        right.absoluteProbability(PathKey("a")) should equal (0.4285714285 +- 0.0000000001)
        right.absoluteProbability(PathKey("a.b")) should equal ((0.4285714285/3) +- 0.0000000001)

        // test structure of right
        val nodeA = right.children("a")._2
        val nodeB = nodeA.children("b")._2
        val newGraph = right.copy(children = Map("a" -> (1d, nodeA.copy(children = Map("b" -> (1d, nodeB))))))
        newGraph should equal (RSIGraph(("a", 1d, RSIGraph(("b", 1d, RSIGraph(leafMetadata = Some(ColumnMetadata(5, 9, 3))))))))
      }
    }
    "give its path options" when {
      "it is a presence split" in {
        presenceCandidate.paths should equal ((Present, Absent))
      }
      "it is a median split" in {
        medianCandidate.paths should equal ((Less, MoreOrNull))
      }
    }
    "calculate the fraction of data in its left child" when {
      "it is a presence split" in {
        presenceCandidate.leftFraction(graph) should equal (.5)
      }
      "it is a median split" in {
        medianCandidate.leftFraction(graph) should equal (.125)
      }
    }
    "return the shape it creates" when {
      "it is a presence split" in {
        val res = presenceCandidate.shape(df,graph).asInstanceOf[SplitByPresence[DataFrame]]
        res.shape should equal (SplitByPresence("a", (), ()))
        res.presentKey.asInstanceOf[Bucket[DataFrame]].data.collect should contain theSameElementsAs (df.filter(col("a").isNotNull).collect())
        res.absentKey.asInstanceOf[Bucket[DataFrame]].data.collect should contain theSameElementsAs (df.filter(col("a").isNull).collect())
      }
      "it is a median split" in {
        Given("A graph and split candidate")
        val graph = RSIGraph(("a", .5, RSIGraph(leafMetadata = Some(ColumnMetadata(0, 9, 6)))))
        val candidate = MedianSplitCandidate(PathKey("a"))

        When("we split a dataframe according to them")
        val res = candidate.shape(df,graph).asInstanceOf[SplitByValue[DataFrame]]

        Then("the shape has the right shape")
        res.shape should equal (SplitByValue(4, "a", (), ()))
        res.less.asInstanceOf[Bucket[DataFrame]].data.collect() should contain theSameElementsAs (df.filter(col("a") <= 4).collect())
        res.more.asInstanceOf[Bucket[DataFrame]].data.collect() should contain theSameElementsAs (df.filter(col("a").isNull || col("a") > 4).collect())
      }
    }
  }

  val presenceCandidate = PresenceSplitCandidate(PathKey("a"))
  val medianCandidate = MedianSplitCandidate(PathKey("a.b"))

  val graph = RSIGraph(("a", .5, RSIGraph(("b", .5, RSIGraph(leafMetadata = Some(ColumnMetadata(0, 9, 6)))))))
}