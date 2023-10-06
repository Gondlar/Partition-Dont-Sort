package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.partitions.{SplitByPresence,Present,Absent,Bucket}
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.util.UniformColumnMetadata
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.Versions
import de.unikl.cs.dbis.waves.util.Leaf

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame

class SplitCandidateStateSpec extends WavesSpec
  with DataFrameFixture {

  "The SplitCandidateState" can {
    "be constructed correctly" in {
      state should have (
        'split (candidate),
        'graph (graph),
        'priority (5),
        'path (Seq(Present, Absent))
      )
    }
    "return the shape it creates" in {
      val res = state.splitShape(df).asInstanceOf[SplitByPresence[DataFrame]]
      res.shape should equal (SplitByPresence("a", (), ()))
      res.presentKey.asInstanceOf[Bucket[DataFrame]].data.collect should contain theSameElementsAs (df.filter(col("a").isNotNull).collect())
      res.absentKey.asInstanceOf[Bucket[DataFrame]].data.collect should contain theSameElementsAs (df.filter(col("a").isNull).collect())
    }
    "return its children" in {
      val (right, left) = graph.splitBy(PathKey("a")).value
      val (resLeft, resRight) = state.children
      resLeft should equal ((left, Seq(Present, Absent, Present)))
      resRight should equal ((right, Seq(Present, Absent, Absent)))
    }
  }

  val candidate = PresenceSplitCandidate(PathKey("a"))
  val graph = Versions(
          IndexedSeq("a"),
          IndexedSeq(Leaf(Some(UniformColumnMetadata(0, 9, 6)))),
          Seq((IndexedSeq(true), .5),(IndexedSeq(false), .5))
        )
  val state = SplitCandidateState(candidate, graph, 1, 5, Seq(Present, Absent))
}