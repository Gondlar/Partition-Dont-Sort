package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.partitions._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.split.recursive.ColumnMetadata
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.Versions
import de.unikl.cs.dbis.waves.util.Leaf

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame


class SplitCandidateSpec extends WavesSpec
  with DataFrameFixture {

  "The SplitCandidate" can {
    "split a graph" when {
      "it is a presence split" when {
        "it is a valid location" in {
          presenceCandidate.split(graph).value should equal ((
            testGraph(1, .5),
            testGraph(0, .5)
          ))
        }
        "it is an invalid location" in {
          presenceCandidate.split(testGraph(0, .5)) shouldBe ('left)
          presenceCandidate.split(testGraph(1, .5)) shouldBe ('left)
        }
      }
      "it is a median split" in {
        val (left, right) = medianCandidate.split(graph).value
        left should equal (testGraph(1, 1, 0, 4, 3))
        right.absoluteProbability(PathKey("a")) should equal (0.4285714285 +- 0.0000000001)
        right.absoluteProbability(PathKey("a.b")) should equal ((0.4285714285/3) +- 0.0000000001)
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
        val graph = Versions(
          IndexedSeq("a"),
          IndexedSeq(Leaf(Some(ColumnMetadata(0, 9, 6)))),
          makeVersions(.5)
        )
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

  def testGraph(probA: Double, probB: Double, min: Int = 0, max: Int = 9, distinct: Long = 6) = Versions(
    IndexedSeq("a"),
    IndexedSeq(
      Versions(
        IndexedSeq("b"),
        IndexedSeq(Leaf(Some(ColumnMetadata(min, max, distinct)))),
        makeVersions(probB)
      )
    ),
    makeVersions(probA)
  )

  def makeVersions(prob: Double) = prob match {
    case 0d => Seq((IndexedSeq(false), 1d))
    case 1d => Seq((IndexedSeq(true), 1d))
    case _ => Seq( (IndexedSeq(true), prob)
                 , (IndexedSeq(false), 1-prob)
                 )
  }

  val graph = testGraph(.5, .5)
}