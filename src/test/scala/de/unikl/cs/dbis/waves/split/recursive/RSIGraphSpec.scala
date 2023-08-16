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
    "be constructed from an ObjectCounter" in {
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
    "determine whether a path is certain" when {
      "it is required" in {
        val path = PathKey("foo")
        RSIGraph(("foo", 1d, RSIGraph.empty)).isCertain(path) shouldBe (true)
      }
      "it is always missing" in {
        val path = PathKey("foo")
        RSIGraph(("foo", 0d, RSIGraph.empty)).isCertain(path) shouldBe (true)
      }
      "it is nested" in {
        val path = PathKey("foo.bar")
        val graph = RSIGraph(("foo", 1d, RSIGraph(("bar", 0.5, RSIGraph.empty))))
        graph.isCertain(path) shouldBe (false)
      }
      "it is not in the graph" in {
        val path = PathKey("foo")
        RSIGraph.empty.isCertain(path) shouldBe (true)
      }
    }
    "give absolute probabilities for paths" when {
      "the path exists" in {
        val graph = RSIGraph(("foo", 0.25, RSIGraph(("bar", .5, RSIGraph(("baz", 0.75, RSIGraph.empty))))))
        graph.absoluteProbability(PathKey("foo.bar")) should equal (0.125)
      }
      "the path does not exist" in {
        val graph = RSIGraph(("foo", 0.25, RSIGraph(("bar", .5, RSIGraph(("baz", 0.75, RSIGraph.empty))))))
        graph.absoluteProbability(PathKey("bar")) should equal (0)
      }
    }
    "create RSIGraphs resulting from a split by presence" in {
      Given("An RSIGraph and a split path")
      val graph = RSIGraph(
        ("a", 0.25, RSIGraph(
          ("b", 1d, RSIGraph.empty),
          ("c", 0.75, RSIGraph(
            ("d", 1d, RSIGraph.empty),
            ("e", 1d, RSIGraph.empty)
          ))
        )),
        ("f", 0.75, RSIGraph.empty)
      )
      val path = PathKey("a.c")

      When("we split it")
      val (absent, present) = graph.splitBy(path)

      Then("the splits should be correct")
      val absentGraph = RSIGraph(
        ("a", 0.25, RSIGraph(
          ("b", 1d, RSIGraph.empty)
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
    "calculate its gini index" when {
      "it is just the root" in {
        val graph = RSIGraph.empty
        graph.gini should equal (0)
      }
      "it has one leaf" in {
        val graph = RSIGraph(("foo", 0.25, RSIGraph(("bar", 1d, RSIGraph(("baz", 0.75, RSIGraph.empty))))))
        graph.gini should equal (0.3984375)
      }
      "it has multiple leaves" in {
        val graph = RSIGraph(
          ("a", 0.25, RSIGraph(
            ("b", 1d, RSIGraph.empty),
            ("c", 0.5, RSIGraph(
              ("d", 1d, RSIGraph.empty),
              ("e", 1d, RSIGraph.empty)
            ))
          )),
          ("f", 0.75, RSIGraph.empty)
        )

        graph.gini should equal (1.5625)
      }
    }
  }
  it should {
    "have the correct empty representation" in {
      RSIGraph.empty should equal (RSIGraph())
    }
  }
}