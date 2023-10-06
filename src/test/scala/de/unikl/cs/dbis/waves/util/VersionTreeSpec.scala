package de.unikl.cs.dbis.waves.util

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SchemaFixture

class VersionTreeSpec extends WavesSpec with SchemaFixture {

  "A VersionTree" can {
    "not be created" when {
      "children and names do not fit" in {
        an [AssertionError] shouldBe thrownBy (Versions(IndexedSeq("test"), IndexedSeq.empty, Seq((IndexedSeq(true), 1d))))
      }
      "child names are not sorted" in {
        an [AssertionError] shouldBe thrownBy (Versions(IndexedSeq("test", "abc"), IndexedSeq(Leaf.empty, Leaf.empty), Seq((IndexedSeq(true, true), 1d))))
      }
      "are no versions" in {
        an [AssertionError] shouldBe thrownBy (Versions(IndexedSeq("test"), IndexedSeq.empty, Seq.empty))
      }
      "a version signature does not fit to the children" in {
        an [AssertionError] shouldBe thrownBy (Versions(IndexedSeq("test"), IndexedSeq(Leaf.empty), Seq((IndexedSeq(true, true), 1d))))
      }
      "there are duplicate versions" in {
        an [AssertionError] shouldBe thrownBy (Versions(IndexedSeq("test"), IndexedSeq(Leaf.empty), Seq((IndexedSeq(true), .5), (IndexedSeq(true), .5))))
      }
      "there is a version with zero or less probability" in {
        an [AssertionError] shouldBe thrownBy (Versions(IndexedSeq("test"), IndexedSeq(Leaf.empty), Seq((IndexedSeq(true), 1), (IndexedSeq(false), 0))))
        an [AssertionError] shouldBe thrownBy (Versions(IndexedSeq("test"), IndexedSeq(Leaf.empty), Seq((IndexedSeq(true), 1), (IndexedSeq(false), -234))))
      }
      "the version probabilities do not sum to 1" in {
        an [AssertionError] shouldBe thrownBy (Versions(IndexedSeq("test"), IndexedSeq(Leaf.empty), Seq((IndexedSeq(true), .5), (IndexedSeq(false), .3))))
      }
    }
    "determine whether a path is certain" when {
      "it is a leaf" in {
        val path = PathKey("foo")
        val graph = Leaf.empty
        graph.isCertain(path) shouldBe (true)
        graph.isValidSplitLocation(path) shouldBe (false)
      }
      "it is required" in {
        val path = PathKey("foo")
        val graph = (Versions(IndexedSeq("foo"), IndexedSeq(Leaf.empty), Seq((IndexedSeq(true), 1d))))
        graph.isCertain(path) shouldBe (true)
        graph.isValidSplitLocation(path) shouldBe (false)
      }
      "it is always missing" in {
        val path = PathKey("foo")
        val graph = (Versions(IndexedSeq("foo"), IndexedSeq(Leaf.empty), Seq((IndexedSeq(false), 1d))))
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
      "it is a leaf" in {
        Leaf.empty.isLeaf(PathKey("foo.bar.baz")) shouldBe (false)
        Leaf.empty.isLeaf(PathKey("foo.bar.baz"), true) shouldBe (false)
      }
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
      "it is a leaf" in {
        val path = Some(PathKey("foo.bar.baz"))
        val metadata = ColumnMetadata(0, 10, 4)
        Leaf.empty.setMetadata(path, metadata) shouldBe ('left)
      }
      "the path is valid" in {
        val path = Some(PathKey("foo.bar.baz"))
        val metadata = ColumnMetadata(0, 10, 4)
        val expected = Versions(
          IndexedSeq("foo"),
          IndexedSeq(Versions(
            IndexedSeq("bar"),
            IndexedSeq(Versions(
              IndexedSeq("baz"),
              IndexedSeq(Leaf(Some(ColumnMetadata(0, 10, 4)))),
              Seq((IndexedSeq(false), .25), (IndexedSeq(true), .75))
            )),
            Seq((IndexedSeq(true), 1d))  
          )),
          Seq((IndexedSeq(false), .75), (IndexedSeq(true), .25))
        )
        lineGraph.setMetadata(path, metadata).value should equal (expected)
      }
      "the path does not exist" in {
        val path = Some(PathKey("bar"))
        val metadata = ColumnMetadata(0, 10, 4)
        lineGraph.setMetadata(path, metadata) shouldBe ('left)
      }
      "the path is not a leaf" in {
        val path = Some(PathKey("foo.bar"))
        val metadata = ColumnMetadata(0, 10, 4)
        lineGraph.setMetadata(path, metadata) shouldBe ('left)
      }
    }
    "give absolute probabilities for paths" when {
      "it is a leaf" in {
        Leaf.empty.absoluteProbability(PathKey("foo")) should equal (0)
      }
      "the path exists" in {
        lineGraph.absoluteProbability(PathKey("foo.bar")) should equal (0.25)
      }
      "the path does not exist" in {
        lineGraph.absoluteProbability(PathKey("bar")) should equal (0)
      }
    }
    "determine the separator for a leaf" when {
      "it is a leaf" in {
        Leaf.empty.separatorForLeaf(Some(PathKey("foo"))) shouldBe ('left)
      }
      "the path does not lead to a leaf" in {
        bushyGraphWithMetadata.separatorForLeaf(Some(PathKey("a")), .75) shouldBe ('left)
      }
      "the path has metadata" in {
        bushyGraphWithMetadata.separatorForLeaf(Some(PathKey("a.b")), .75).value should equal (IntegerColumn(11))
      }
      "the path has no metadata" in {
        bushyGraphWithMetadata.separatorForLeaf(Some(PathKey("f")), .75) shouldBe 'left
      }
      "the path is invalid" in {
        bushyGraphWithMetadata.separatorForLeaf(Some(PathKey("foobar")), .75) shouldBe 'left
      }
    }
    "create VersionTrees resulting from a split by presence" when {
      "it is a leaf" in {
        Leaf.empty.splitBy(PathKey("foo")) shouldBe ('left)
      }
      "the path is invalid" in {
        bushyGraph.splitBy(PathKey("foo")) shouldBe ('left)
      }
      "the path is always absent" in {
        val graph = Versions(
          IndexedSeq("foo"),
          IndexedSeq(Leaf.empty),
          Seq((IndexedSeq(false), 1d))
        )
        graph.splitBy(PathKey("foo")) shouldBe ('left)
      }
      "it is a valid path" in {
        Given("A VersionTree and a split path")
        val graph = bushyGraph
        val path = PathKey("a.c")

        When("we split it")
        val (absent, present) = graph.splitBy(path).value

        Then("the splits should be correct")
        val absentGraph = Versions(
          IndexedSeq("a", "f"),
          IndexedSeq(
            Versions(
              IndexedSeq("b", "c"),
              IndexedSeq(
                Leaf.empty,
                Versions(
                  IndexedSeq("d", "e"),
                  IndexedSeq(
                    Leaf.empty,
                    Leaf.empty
                  ),
                  Seq((IndexedSeq(true, true), 1d))
                )
              ),
              Seq((IndexedSeq(true, false), 1d))
            ),
            Leaf.empty
          ),
          Seq((IndexedSeq(true, false), 0.14285714285714285), (IndexedSeq(false, true), 0.8571428571428571))
        )
        val presentGraph = Versions(
          IndexedSeq("a", "f"),
          IndexedSeq(
            Versions(
              IndexedSeq("b", "c"),
              IndexedSeq(
                Leaf.empty,
                Versions(
                  IndexedSeq("d", "e"),
                  IndexedSeq(
                    Leaf.empty,
                    Leaf.empty
                  ),
                  Seq((IndexedSeq(true, true), 1d))
                )
              ),
              Seq((IndexedSeq(true, true), 1d))
            ),
            Leaf.empty
          ),
          Seq((IndexedSeq(true, false), 1d))
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
        lineGraph.splitBy(PathKey("foo.bar.baz"), 0).left.value should equal ("0 < quantile < 1 must hold")
        lineGraph.splitBy(PathKey("foo.bar.baz"), 1).left.value should equal ("0 < quantile < 1 must hold")
        Leaf.empty.splitBy(PathKey("foo.bar.baz"), 0).left.value should equal ("0 < quantile < 1 must hold")
        Leaf.empty.splitBy(PathKey("foo.bar.baz"), 1).left.value should equal ("0 < quantile < 1 must hold")
      }
      "the leaf is never present" in {
        lineGraph.splitBy(PathKey("bar"), .5) shouldBe ('left)
      }
      "the path does not lead to a leaf" in {
        lineGraph.splitBy(PathKey("foo"), .5) shouldBe ('left)
      }
      "there is no metadata" in {
        Leaf.empty.splitBy(None, .5) shouldBe ('left)
      }
      "the path is always absent" in {
        val graph = Versions(
          IndexedSeq("foo"),
          IndexedSeq(Leaf(Some(ColumnMetadata(12, 14, 2)))),
          Seq((IndexedSeq(false), 1d))
        )
        graph.splitBy(PathKey("foo"), 0.5) shouldBe ('left)
      }
      "it is a valid split" in {
        Given("a graph and a root-to-leaf path")
        val path = PathKey("a.b")

        When("we split it")
        val (trueSplit, falseSplit) = bushyGraphWithMetadata.splitBy(path, 0.75).value

        Then("they look as expected")
        val trueGraph = Versions(
          IndexedSeq("a", "f"),
          IndexedSeq(
            Versions(
              IndexedSeq("b", "c"),
              IndexedSeq(
                Leaf(Some(ColumnMetadata(3, 11, 3))),
                Versions(
                  IndexedSeq("d", "e"),
                  IndexedSeq(
                    Leaf.empty,
                    Leaf.empty
                  ),
                  Seq((IndexedSeq(true, true), 1d))
                )
              ),
              Seq((IndexedSeq(true, false), .5), (IndexedSeq(true, true), .5))
            ),
            Leaf.empty
          ),
          Seq((IndexedSeq(true, false), 1d))
        )
        trueSplit should equal (trueGraph)
        
        val falseGraph = Versions(
          IndexedSeq("a", "f"),
          IndexedSeq(
            Versions(
              IndexedSeq("b", "c"),
              IndexedSeq(
                Leaf(Some(ColumnMetadata(12, 14, 1))),
                Versions(
                  IndexedSeq("d", "e"),
                  IndexedSeq(
                    Leaf.empty,
                    Leaf.empty
                  ),
                  Seq((IndexedSeq(true, true), 1d))
                )
              ),
              Seq((IndexedSeq(true, false), .5), (IndexedSeq(true, true), .5))
            ),
            Leaf.empty
          ),
          Seq((IndexedSeq(true, false), 0.07692307692307693), (IndexedSeq(false, true), 0.9230769230769231))
        )
        falseSplit should equal (falseGraph)
      }
    }
    "calculate its gini index" when {
      "it is just the root" in {
        val graph = Leaf.empty
        (graph.gini: Double) should equal (0)
      }
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
        val graph = Versions(
          IndexedSeq("foo"),
          IndexedSeq(Leaf(Some(ColumnMetadata(0, 10, 4)))),
          Seq((IndexedSeq(false), 1d))
        )
        (graph.gini: Double) should equal (0)
      }
    }
  }
  it should {
    "have the correct empty representation" in {
      Leaf.empty should equal (Leaf())
    }
  }

  
  val lineGraph = Versions(
    IndexedSeq("foo"),
    IndexedSeq(Versions(
      IndexedSeq("bar"),
      IndexedSeq(Versions(
        IndexedSeq("baz"),
        IndexedSeq(Leaf.empty),
        Seq((IndexedSeq(false), .25), (IndexedSeq(true), .75))
      )),
      Seq((IndexedSeq(true), 1d))  
    )),
    Seq((IndexedSeq(false), .75), (IndexedSeq(true), .25))
  )
  val bushyGraph = Versions(
    IndexedSeq("a", "f"),
    IndexedSeq(
      Versions(
        IndexedSeq("b", "c"),
        IndexedSeq(
          Leaf.empty,
          Versions(
            IndexedSeq("d", "e"),
            IndexedSeq(
              Leaf.empty,
              Leaf.empty
            ),
            Seq((IndexedSeq(true, true), 1d))
          )
        ),
        Seq((IndexedSeq(true, false), .5), (IndexedSeq(true, true), .5))
      ),
      Leaf.empty
    ),
    Seq((IndexedSeq(true, false), .25), (IndexedSeq(false, true), .75))
  )
  val bushyGraphWithMetadata = bushyGraph.setMetadata(Some(PathKey("a.b")), ColumnMetadata(3, 14, 4)).right.get
}