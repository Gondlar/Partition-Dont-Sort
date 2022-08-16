package de.unikl.cs.dbis.waves.split.recursive

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.Schema
import de.unikl.cs.dbis.waves.Spark
import de.unikl.cs.dbis.waves.util.PathKey

import org.apache.spark.sql.types.StructType
import de.unikl.cs.dbis.waves.DataFrame

class AutosplitCalculatorSpec extends WavesSpec
    with DataFrame {

    "An AutosplitCalculator" when {
        "filtering known paths" should {
            "filter known absent paths" in {
                filterKnownPaths(Seq(PathKey("foo")), Seq.empty, PathKey("foo.bar")) should be (false)
                filterKnownPaths(Seq(PathKey("foo")), Seq.empty, PathKey("foo")) should be (false)
            }
            "filter known present paths" in {
                filterKnownPaths(Seq.empty, Seq(PathKey("foo")), PathKey("foo")) should be (false)
            }
            "keep other paths" in {
                filterKnownPaths(Seq.empty, Seq(PathKey("foo")), PathKey("foo.bar")) should be (true)
                filterKnownPaths(Seq.empty, Seq(PathKey("foo")), PathKey("bar")) should be (true)
                filterKnownPaths(Seq(PathKey("foo")), Seq.empty, PathKey("bar")) should be (true)
            } 
        }
        "calculating" should {
            "find the correct metrics for all Rows" in {
                val res = calculate(df, Seq.empty, Seq.empty, 0)
                res should equal (Seq(
                    (PathKey("a"),   0, 0),
                    (PathKey("b"),   0, 4),
                    (PathKey("b.d"), 2, 2)
                ))
            }

            "skip known absent subtrees" in {
                val res = calculate(df, Seq(PathKey("b")), Seq.empty, 0)
                res should equal (Seq(
                    (PathKey("a"),   0, 0)
                ))
            }

            "skip known present paths" in {
                val res = calculate(df, Seq.empty, Seq(PathKey("b")), 0)
                res should equal (Seq(
                    (PathKey("a"),   0, 0),
                    (PathKey("b.d"), 2, 2)
                ))
            }

            "skip paths outside threshold" in {
                val res = calculate(df, Seq.empty, Seq.empty, 3/8d)
                res should equal (Seq(
                    (PathKey("a"),   0, 0),
                    (PathKey("b"),   0, 4)
                ))
            }
        }
        "using the switch heuristic" should {
            "find the document with the highest value" in {
                val res = switchHeuristic(df, Seq.empty, Seq.empty, 0)
                res should equal (Some(PathKey("b")))
            }
            "find no document when no paths are allowable" in {
                val res = switchHeuristic(emptyDf, Seq.empty, Seq.empty, 0)
                res should equal (None)
            }
        }
        "using the even heuristic" should {
            "find the document with the lowest value" in {
                val res = evenHeuristic(df, Seq.empty, Seq.empty, 0)
                res should (equal (Some(PathKey("a"))) or equal (Some(PathKey("b"))) )
            }
            "find no document when no paths are allowable" in {
                val res = evenHeuristic(emptyDf, Seq.empty, Seq.empty, 0)
                res should equal (None)
            }
        }
    }
    
}