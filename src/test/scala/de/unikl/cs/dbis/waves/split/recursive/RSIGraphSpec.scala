package de.unikl.cs.dbis.waves.split.recursive

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SchemaFixture

import de.unikl.cs.dbis.waves.util.PathKey

class RSIGraphSpec extends WavesSpec with SchemaFixture {

  "An RSIGraph" can {
    "reject non-percentage weights when being created" when {
      "they are larger-than-one" in {
        an [AssertionError] shouldBe thrownBy (RSIGraph(("foo", 2d, RSIGraph.empty)))
      }
      "they are smaller-than-zero" in {
        an [AssertionError] shouldBe thrownBy (RSIGraph(("foo", -1d, RSIGraph.empty)))
      }
    }
    "be constructed from an ObjectCounter" when {
      "all subtrees are present at least once" in {
        val total = 12
        val counter = new ObjectCounter(Array(9, 6, 3))

        val graph = RSIGraph.fromObjectCounter(counter, schema, total)
        graph should equal (RSIGraph(
          ("a", .75, RSIGraph.empty),
          ("b", .5, RSIGraph(
            ("c", 1d, RSIGraph.empty),
            ("d", .5, RSIGraph.empty)
          )),
          ("e", 1d, RSIGraph.empty)
        ))
      }
      "there is a subtree that is never present" in {
        val total = 12
        val counter = new ObjectCounter(Array(9, 0, 0))

        val graph = RSIGraph.fromObjectCounter(counter, schema, total)
        graph should equal (RSIGraph(
          ("a", .75, RSIGraph.empty),
          ("b", 0, RSIGraph(
            ("c", 1d, RSIGraph.empty),
            ("d", 0d, RSIGraph.empty)
          )),
          ("e", 1d, RSIGraph.empty)
        ))
      }
    }
    "determine whether a path is certain" when {
      "it is required" in {
        val path = PathKey("foo")
        val graph = RSIGraph(("foo", 1d, RSIGraph.empty))
        graph.isCertain(path) shouldBe (true)
        graph.isValidSplitLocation(path) shouldBe (false)
      }
      "it is always missing" in {
        val path = PathKey("foo")
        val graph = RSIGraph(("foo", 0d, RSIGraph.empty))
        graph.isCertain(path) shouldBe (true)
        graph.isValidSplitLocation(path) shouldBe (false)
      }
      "it is nested" in {
        val path = PathKey("foo.bar")
        val graph = RSIGraph(("foo", 1d, RSIGraph(("bar", 0.5, RSIGraph.empty))))
        graph.isCertain(path) shouldBe (false)
        graph.isValidSplitLocation(path) shouldBe (true)
      }
      "it is not in the graph" in {
        val path = PathKey("foo")
        val graph = RSIGraph.empty
        graph.isCertain(path) shouldBe (true)
        graph.isValidSplitLocation(path) shouldBe (false)
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
        val metadata = IntColumnMetadata(0, 10, 4)
        lineGraph.setMetadata(path, metadata).value should equal (
          RSIGraph(("foo", 0.25, RSIGraph(("bar", 1d, RSIGraph(("baz", 0.75, RSIGraph(Map.empty[String, (Double, RSIGraph)], Some(metadata))))))))
        )
      }
      "the path does not exist" in {
        val path = Some(PathKey("bar"))
        val metadata = IntColumnMetadata(0, 10, 4)
        lineGraph.setMetadata(path, metadata).left.value should equal ("bar is not a valid path")
      }
      "the path is not a leaf" in {
        val path = Some(PathKey("foo.bar"))
        val metadata = IntColumnMetadata(0, 10, 4)
        lineGraph.setMetadata(path, metadata).left.value should equal ("path is not a leaf")
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
      "the path has metadata" in {
        bushyGraphWithMetadata.separatorForLeaf(Some(PathKey("a.b")), .75).value should equal (11)
      }
      "the path has no metadata" in {
        bushyGraphWithMetadata.separatorForLeaf(Some(PathKey("f")), .75) shouldBe 'left
      }
      "the path is invalid" in {
        bushyGraphWithMetadata.separatorForLeaf(Some(PathKey("foobar")), .75) shouldBe 'left
      }
    }
    "create RSIGraphs resulting from a split by presence" when {
      "it is a valid path" in {
        Given("An RSIGraph and a split path")
        val graph = bushyGraph
        val path = PathKey("a.c")

        When("we split it")
        val (absent, present) = graph.splitBy(path).value

        Then("the splits should be correct")
        val absentGraph = RSIGraph(
          ("a", 0.25, RSIGraph(
            ("b", 1d, RSIGraph.empty),
            ("c", 0d, RSIGraph(
              ("d", 1d, RSIGraph.empty),
              ("e", 1d, RSIGraph.empty)
            ))
          )),
          ("f", 0.75, RSIGraph.empty)
        )
        val presentGraph = RSIGraph(
          ("a", 1d, RSIGraph(
            ("b", 1d, RSIGraph.empty),
            ("c", 1d, RSIGraph(
              ("d", 1d, RSIGraph.empty),
              ("e", 1d, RSIGraph.empty)
            ))
          )),
          ("f", 0.75, RSIGraph.empty)
        )
        present should equal (presentGraph)
        absent should equal (absentGraph)
      }
      "when the path is certain" in {
        bushyGraph.splitBy(PathKey("a.b")) shouldBe 'left
      }
    }
    "create RSIGraphs resulting from a split by percentile" when {
      "the quantile is not a percentage" in {
        lineGraph.splitBy(PathKey("foo.bar.baz"), 0).left.value should equal ("0 < quantile < 1 must hold")
        lineGraph.splitBy(PathKey("foo.bar.baz"), 1).left.value should equal ("0 < quantile < 1 must hold")
      }
      "the leaf is never present" in {
        lineGraph.splitBy(PathKey("bar"), .5).left.value should equal ("bar is always absent")
      }
      "the path does not lead to a leaf" in {
        lineGraph.splitBy(PathKey("foo"), .5).left.value should equal ("foo is not a leaf with metadata")
      }
      "it is a valid split" in {
        Given("a graph and a root-to-leaf path")
        val path = PathKey("a.b")

        When("we split it")
        val (trueSplit, falseSplit) = bushyGraphWithMetadata.splitBy(path, 0.75).value

        Then("they look as expected")
        val trueGraph = RSIGraph(
          ("a", 1d, RSIGraph(
            ("b", 1d, RSIGraph(Map.empty[String,(Double,RSIGraph)], Some(IntColumnMetadata(3, 11, 3)))),
            ("c", 0.5, RSIGraph(
              ("d", 1d, RSIGraph.empty),
              ("e", 1d, RSIGraph.empty)
            ))
          )),
          ("f", 0.75, RSIGraph.empty)
        )
        trueSplit should equal (trueGraph)
        
        val falseGraph = RSIGraph(
          ("a", 0d, RSIGraph(
            ("b", 1d, RSIGraph(Map.empty[String,(Double,RSIGraph)], Some(IntColumnMetadata(12, 14, 1)))),
            ("c", 0.5, RSIGraph(
              ("d", 1d, RSIGraph.empty),
              ("e", 1d, RSIGraph.empty)
            ))
          )),
          ("f", 0.75, RSIGraph.empty)
        )
        // Test probability of a
        falseSplit.absoluteProbability(PathKey("a")) shouldBe (.076923077 +- 0.00000001)
        // Test the structure of the remaining graph
        RSIGraph(falseSplit.children.updated("a", (0d, falseSplit.children("a")._2))) should equal (falseGraph)
      }
    }
    "calculate its gini index" when {
      "it is just the root" in {
        val graph = RSIGraph.empty
        graph.gini should equal (0)
      }
      "it has one leaf" in {
        lineGraph.gini should equal (0.3984375)
      }
      "it has multiple leaves" in {
        bushyGraph.gini should equal (1.5625)
      }
      "it has metadata" in {
        bushyGraphWithMetadata.gini should equal (2.3125)
      }
      "it has metadata for always absent columns" in {
        val graph = RSIGraph(("foo", 0d, RSIGraph(leafMetadata = Some(IntColumnMetadata(0, 10, 4)))))
        graph.gini should equal (0)
      }
    }
  }
  it should {
    "have the correct empty representation" in {
      RSIGraph.empty should equal (RSIGraph())
    }
  }

  val lineGraph = RSIGraph(("foo", 0.25, RSIGraph(("bar", 1d, RSIGraph(("baz", 0.75, RSIGraph.empty))))))
  val bushyGraph = RSIGraph(
      ("a", 0.25, RSIGraph(
        ("b", 1d, RSIGraph.empty),
        ("c", 0.5, RSIGraph(
          ("d", 1d, RSIGraph.empty),
          ("e", 1d, RSIGraph.empty)
        ))
      )),
      ("f", 0.75, RSIGraph.empty)
    )
  val bushyGraphWithMetadata = bushyGraph.setMetadata(Some(PathKey("a.b")), IntColumnMetadata(3, 14, 4)).right.get
}