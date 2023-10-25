package de.unikl.cs.dbis.waves.util

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SchemaFixture

class TotalFingerprintSpec extends WavesSpec with SchemaFixture {

  "A TotalFingerprint" can {
    "not be created" when {
      "the names are not sorted" in {
        an [AssertionError] shouldBe thrownBy (TotalFingerprint(IndexedSeq("bar", "abc"), Set((IndexedSeq(false, false), 10)), IndexedSeq(None), IndexedSeq("bar")))
      }
      "the fingerprints are of the wrong size" in {
        an [AssertionError] shouldBe thrownBy (TotalFingerprint(IndexedSeq("abc", "bar"), Set((IndexedSeq(false), 10)), IndexedSeq(None), IndexedSeq("bar")))
      }
      "the leafs are not sorted" in {
        an [AssertionError] shouldBe thrownBy (TotalFingerprint(IndexedSeq("abc", "bar"), Set((IndexedSeq(false, false), 10)), IndexedSeq(None), IndexedSeq("bar", "abc")))
      }
      "the leafs are not a subset of the nodes" in {
        an [AssertionError] shouldBe thrownBy (TotalFingerprint(IndexedSeq("abc", "bar"), Set((IndexedSeq(false, false), 10)), IndexedSeq(None, None), IndexedSeq("bar", "foo")))
      }
      "the metadata is of the wrong size" in {
        an [AssertionError] shouldBe thrownBy (TotalFingerprint(IndexedSeq("bar", "abc"), Set((IndexedSeq(false, false), 10)), IndexedSeq(None, None), IndexedSeq("bar")))
      }
    }
    "determine whether a path is certain" when {
      "it is a leaf" in {
        val path = PathKey("foo")
        val graph = bushyGraph
        graph.isCertain(path) shouldBe (true)
        graph.isValidSplitLocation(path) shouldBe (false)
      }
      "it is certain" in {
        val path = PathKey("a.b")
        bushyGraph.isCertain(path) shouldBe (true)
        bushyGraph.isValidSplitLocation(path) shouldBe (false)
      }
      "it is required" in {
        val path = PathKey("foo")
        val graph = TotalFingerprint(IndexedSeq("foo"), Set((IndexedSeq(true), 5)), IndexedSeq(None), IndexedSeq("foo"))
        graph.isCertain(path) shouldBe (true)
        graph.isValidSplitLocation(path) shouldBe (false)
      }
      "it is always missing" in {
        val path = PathKey("foo")
        val graph = TotalFingerprint(IndexedSeq("foo"), Set((IndexedSeq(false), 5)), IndexedSeq(None), IndexedSeq("foo"))
        graph.isCertain(path) shouldBe (true)
        graph.isValidSplitLocation(path) shouldBe (false)
      }
      "it is nested" in {
        val path = PathKey("foo.bar.baz")
        lineGraph.isCertain(path) shouldBe (false)
        lineGraph.isValidSplitLocation(path) shouldBe (true)
      }
      "it is not in the graph" in {
        val path = PathKey("foobar")
        lineGraph.isCertain(path) shouldBe (true)
        lineGraph.isValidSplitLocation(path) shouldBe (false)
      }
    }
    "determine whether a path leads to a leaf" when {
      "it leads to a leaf" in {
        lineGraph.isLeaf(PathKey("foo.bar.baz")) shouldBe (true)
        lineGraph.isLeaf(PathKey("foo.bar.baz"), true) shouldBe (false)
      }
      "it leads to an inner node" in {
        lineGraph.isLeaf(PathKey("foo.bar")) shouldBe (false)
        lineGraph.isLeaf(PathKey("foo.bar"), true) shouldBe (false)
      }
      "it leads to a non-existing node" in {
        lineGraph.isLeaf(PathKey("bar")) shouldBe (false)
        lineGraph.isLeaf(PathKey("bar"), true) shouldBe (false)
      }
      "it leads to a leaf with metadata" in {
        bushyGraphWithMetadata.isLeaf(PathKey("a.b")) shouldBe (true)
        bushyGraphWithMetadata.isLeaf(PathKey("a.b"), true) shouldBe (true)
      }
    }
    "set metadata" when {
      "the path is valid" in {
        val path = Some(PathKey("foo.bar.baz"))
        val metadata = UniformColumnMetadata(0, 10, 4)
        val expected = TotalFingerprint(
          IndexedSeq("foo", "foo.bar", "foo.bar.baz"),
          Set(
            (IndexedSeq(false, false, false), 300L),
            (IndexedSeq(true, true, false), 25L),
            (IndexedSeq(true, true, true), 75L)
          ),
          IndexedSeq(Some(metadata)),
          IndexedSeq("foo.bar.baz")
        )
        lineGraph.setMetadata(path, metadata).value should equal (expected)
      }
      "the path does not exist" in {
        val path = Some(PathKey("bar"))
        val metadata = UniformColumnMetadata(0, 10, 4)
        lineGraph.setMetadata(path, metadata) shouldBe ('left)
      }
      "the path is not a leaf" in {
        val path = Some(PathKey("foo.bar"))
        val metadata = UniformColumnMetadata(0, 10, 4)
        lineGraph.setMetadata(path, metadata) shouldBe ('left)
      }
    }
    "give absolute probabilities for paths" when {
      "the path exists" in {
        lineGraph.absoluteProbability(PathKey("foo.bar")) should equal (0.25)
      }
      "the path does not exist" in {
        lineGraph.absoluteProbability(PathKey("bar")) should equal (0)
      }
    }
    "determine the separator for a leaf" when {
      "the path does not lead to a leaf" in {
        bushyGraphWithMetadata.separatorForLeaf(Some(PathKey("a")), .75) shouldBe ('left)
      }
      "the path has metadata" in {
        bushyGraphWithMetadata.separatorForLeaf(Some(PathKey("a.b")), .75).value should equal ((IntegerColumn(11), .75))
      }
      "the path has no metadata" in {
        bushyGraphWithMetadata.separatorForLeaf(Some(PathKey("f")), .75) shouldBe 'left
      }
      "the path is invalid" in {
        bushyGraphWithMetadata.separatorForLeaf(Some(PathKey("foobar")), .75) shouldBe 'left
      }
    }
    "create VersionTrees resulting from a split by presence" when {
      "the path is invalid" in {
        bushyGraph.splitBy(PathKey("foo")) shouldBe ('left)
      }
      "the path is always absent" in {
        val graph = alwaysAbsent
        graph.splitBy(PathKey("foo")) shouldBe ('left)
      }
      "it is a valid path" in {
        Given("A VersionTree and a split path")
        val graph = bushyGraph
        val path = PathKey("a.c")

        When("we split it")
        val (absent, present) = graph.splitBy(path).value

        Then("the splits should be correct")
        val absentGraph = TotalFingerprint(
          IndexedSeq("a", "a.b", "a.c", "a.c.d", "a.c.e", "f"),
          Set(
            (IndexedSeq(true, true, false, false, false, false), 25),
            (IndexedSeq(false, false, false, false, false, true), 150)
          ),
          IndexedSeq(None, None, None, None),
          IndexedSeq("a.b", "a.c.d", "a.c.e", "f")
        )
        val presentGraph = TotalFingerprint(
          IndexedSeq("a", "a.b", "a.c", "a.c.d", "a.c.e", "f"),
          Set(
            (IndexedSeq(true, true, true, true, true, false), 25)
          ),
          IndexedSeq(None, None, None, None),
          IndexedSeq("a.b", "a.c.d", "a.c.e", "f")
        )
        present should equal (presentGraph)
        absent should equal (absentGraph)
      }
      "when the path is certain" in {
        bushyGraph.splitBy(PathKey("a.b")) shouldBe 'left
      }
    }
    "create VersionTrees resulting from a split by percentile" when {
      "the quantile is not a percentage" in {
        bushyGraphWithMetadata.splitBy(PathKey("a.b"), 0).left.value should equal ("0 < quantile < 1 must hold")
        bushyGraphWithMetadata.splitBy(PathKey("a.b"), 1).left.value should equal ("0 < quantile < 1 must hold")
      }
      "the leaf is never present" in {
        lineGraph.splitBy(PathKey("bar"), .5) shouldBe ('left)
      }
      "the path does not lead to a leaf" in {
        lineGraph.splitBy(PathKey("foo"), .5) shouldBe ('left)
      }
      "there is no metadata" in {
        lineGraph.splitBy(PathKey("foo.bar.baz"), 0.5) shouldBe 'left
      }
      "the path is always absent" in {
        val graph = alwaysAbsent
        graph.splitBy(PathKey("foo"), 0.5) shouldBe ('left)
      }
      "it is a valid split" in {
        Given("a graph and a root-to-leaf path")
        val path = PathKey("a.b")

        When("we split it")
        val (trueSplit, falseSplit) = bushyGraphWithMetadata.splitBy(path, 0.75).value

        Then("they look as expected")
        val trueGraph = TotalFingerprint(
          IndexedSeq("a", "a.b", "a.c", "a.c.d", "a.c.e", "f"),
          Set(
            (IndexedSeq(true, true, true, true, true, false), 18),
            (IndexedSeq(true, true, false, false, false, false), 18),
          ),
          IndexedSeq(Some(UniformColumnMetadata(3, 11, 3)), None, None, None),
          IndexedSeq("a.b", "a.c.d", "a.c.e", "f")
        )
        trueSplit should equal (trueGraph)
        
        val falseGraph = TotalFingerprint(
          IndexedSeq("a", "a.b", "a.c", "a.c.d", "a.c.e", "f"),
          Set(
            (IndexedSeq(false, false, false, false, false, true), 150),
            (IndexedSeq(true, true, true, true, true, false), 7),
            (IndexedSeq(true, true, false, false, false, false), 7),
          ),
          IndexedSeq(Some(UniformColumnMetadata(12, 14, 1)), None, None, None),
          IndexedSeq("a.b", "a.c.d", "a.c.e", "f")
        )
        falseSplit should equal (falseGraph)
      }
    }
    "calculate its gini index" when {
      "it has one leaf" in {
        (lineGraph.gini: Double) should equal (0.3984375)
      }
      "it has multiple leaves" in {
        (bushyGraph.gini: Double) should equal (1.5625)
      }
      "it has metadata" in {
        bushyGraphWithMetadata.gini should equal (2.3125)
      }
      "it has metadata for always absent columns" in {
        val graph = alwaysAbsent
        (graph.gini: Double) should equal (0)
      }
    }
  }

  val alwaysAbsent = TotalFingerprint(
    IndexedSeq("foo"),
    Set(
      (IndexedSeq(false), 50)
    ),
    IndexedSeq(Some(UniformColumnMetadata(12, 14, 2))),
    IndexedSeq("foo")
  )

  val lineGraph = TotalFingerprint(
    IndexedSeq("foo", "foo.bar", "foo.bar.baz"),
    Set(
      (IndexedSeq(false, false, false), 300L),
      (IndexedSeq(true, true, false), 25L),
      (IndexedSeq(true, true, true), 75L)
    ),
    IndexedSeq(None),
    IndexedSeq("foo.bar.baz")
  )

  val bushyGraph = TotalFingerprint(
    IndexedSeq("a", "a.b", "a.c", "a.c.d", "a.c.e", "f"),
    Set(
      (IndexedSeq(true, true, true, true, true, false), 25L),
      (IndexedSeq(true, true, false, false, false, false), 25L),
      (IndexedSeq(false, false, false, false, false, true), 150L),
    ),
    IndexedSeq(None, None, None, None),
    IndexedSeq("a.b", "a.c.d", "a.c.e", "f")
  )
  val bushyGraphWithMetadata = bushyGraph.setMetadata(Some(PathKey("a.b")), UniformColumnMetadata(3, 14, 4)).right.get
}