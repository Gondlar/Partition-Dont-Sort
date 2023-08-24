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
            ("c", 1, RSIGraph.empty),
            ("d", 0, RSIGraph.empty)
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
      }
      "it leads to an inner node" in {
        lineGraph.isLeaf(PathKey("foo.bar")) shouldBe (false)
      }
      "it leads to a non-existing node" in {
        lineGraph.isLeaf(PathKey("bar")) shouldBe (false)
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
    "create RSIGraphs resulting from a split by presence" in {
      Given("An RSIGraph and a split path")
      val graph = bushyGraph
      val path = PathKey("a.c")

      When("we split it")
      val (absent, present) = graph.splitBy(path)

      Then("the splits should be correct")
      val absentGraph = RSIGraph(
        ("a", 0.25, RSIGraph(
          ("b", 1, RSIGraph.empty),
          ("c", 0, RSIGraph(
            ("d", 1, RSIGraph.empty),
            ("e", 1, RSIGraph.empty)
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
    "create RSIGraphs resulting from a split by percentile" when {
      "the quantile is not a percentage" in {
        the [IllegalArgumentException] thrownBy (lineGraph.splitBy(PathKey("foo.bar.baz"), 0)) should have (
          'message ("requirement failed: 0 < quantile < 1")
        )
        the [IllegalArgumentException] thrownBy (lineGraph.splitBy(PathKey("foo.bar.baz"), 1)) should have (
          'message ("requirement failed: 0 < quantile < 1")
        )
      }
      "the leaf is never present" in {
        the [IllegalArgumentException] thrownBy (lineGraph.splitBy(PathKey("bar"), .5)) should have (
          'message ("requirement failed: bar is always absent")
        )
      }
      "the path does not lead to a leaf" in {
        the [IllegalArgumentException] thrownBy (lineGraph.splitBy(PathKey("foo"), .5)) should have (
          'message ("requirement failed: foo is not a leaf")
        )
      }
      "it is a valid split" in {
        Given("a graph and a root-to-leaf path")
        val graph = bushyGraph
        val path = PathKey("a.b")

        When("we split it")
        val (trueSplit, falseSplit) = graph.splitBy(path, 0.75)

        Then("they look as expected")
        val trueGraph = RSIGraph(
          ("a", 1d, RSIGraph(
            ("b", 1d, RSIGraph.empty),
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
            ("b", 1d, RSIGraph.empty),
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
}