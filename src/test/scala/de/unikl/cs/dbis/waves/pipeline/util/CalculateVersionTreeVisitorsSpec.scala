package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import de.unikl.cs.dbis.waves.util.nested.schemas._
import de.unikl.cs.dbis.waves.util.Versions
import de.unikl.cs.dbis.waves.util.Leaf
import de.unikl.cs.dbis.waves.util.BooleanColumnMetadata
import de.unikl.cs.dbis.waves.util.UniformColumnMetadata
import CalculateVersionTree._

class CalculateVersionTreeVisitorsSpec extends WavesSpec {

  "The visitors from CalculateVersionTree" can {
    "visit generate aggregates from a schema" in {
      When("we visit the schema")
      val res = schema(CollectColumnsVisitor())

      Then("The features are as expected")
      res._1 should have length (3)
      res._1(0).expr.toString() should startWith ("array(isnotnull('bool), isnotnull('int), isnotnull('list), isnotnull('map)) AS root-structure-")
      res._1(1).expr.toString() should startWith ("'bool AS bool-")
      res._1(2).expr.toString() should startWith ("'int AS int-")

      And("the results aggregates are correct")
      val featureNames = res._1.map(_.expr.toString().split(" AS ")(1).split("#")(0))
      res._2 should have length (6)
      res._2(0).expr.toString() should startWith (s"collectsetwithcountaggregator('${featureNames(0)}, de.unikl.cs.dbis.waves.util.operators.CollectSetWithCountAggregator")
      res._2(1).expr.toString() should equal (s"count(CASE WHEN NOT '${featureNames(1)} THEN 1 END)")
      res._2(2).expr.toString() should equal (s"count(CASE WHEN '${featureNames(1)} THEN 1 END)")
      res._2(3).expr.toString() should equal (s"min('${featureNames(2)})")
      res._2(4).expr.toString() should equal (s"max('${featureNames(2)})")
      res._2(5).expr.toString() should startWith (s"approx_count_distinct('${featureNames(2)}")
    }
    "build a VersionTree from Aggregates" in {
      val rowSchema = StructType(Array(
        StructField("foo0", StructType(Array(StructField("version", ArrayType(BooleanType)), StructField("count", LongType)))),
        StructField("foo1", LongType),
        StructField("foo2", LongType),
        StructField("foo3", IntegerType),
        StructField("foo4", IntegerType),
        StructField("foo5", LongType),
      ))
      val row = new GenericRowWithSchema(Array(
        Seq(new GenericRow(Array(IndexedSeq(true, true, true, true), 7L))),
        10L,
        30L,
        0,
        10,
        5L
      ), rowSchema)
      val res = schema(MakeVersionedStructureVisitor(row))
      res should equal (
        Versions(
          IndexedSeq("bool", "int", "list", "map"),
          IndexedSeq(
            Leaf(Some(BooleanColumnMetadata(.75))),
            Leaf(Some(UniformColumnMetadata(0, 10, 5))),
            Leaf.empty,
            Leaf.empty
          ),
          Seq((IndexedSeq.fill(4)(true), 1.0))
        )
      )
    }
    "skip nonexistant subtrees" when {
      "visiting a leaf" in {
        val iter = (0 to 10).iterator
        val res = schema.fields(1).dataType(SkipNonexistantSubtreeVisitor(iter))
        res should equal (Leaf.empty)
        iter.next() should equal (3)
      }
      "visiting a boolean leaf" in {
        val iter = (0 to 10).iterator
        val res = schema.fields(0).dataType(SkipNonexistantSubtreeVisitor(iter))
        res should equal (Leaf.empty)
        iter.next() should equal (2)
      }
      "visiting a map" in {
        val iter = (0 to 10).iterator
        val res = schema.fields(2).dataType(SkipNonexistantSubtreeVisitor(iter))
        res should equal (Leaf.empty)
        iter.next() should equal (0)
      }
      "visiting a list" in {
        val iter = (0 to 10).iterator
        val res = schema.fields(3).dataType(SkipNonexistantSubtreeVisitor(iter))
        res should equal (Leaf.empty)
        iter.next() should equal (0)
      }
      "visiting a struct" in {
        val iter = (0 to 10).iterator
        val res = schema(SkipNonexistantSubtreeVisitor(iter))
        res should equal (Versions(
          IndexedSeq("bool", "int", "list", "map"),
          IndexedSeq.fill(4)(Leaf.empty),
          Seq((IndexedSeq.fill(4)(false), 1.0))
        ))
        iter.next() should equal (6)
      }
    }
  }

  val schema = StructType(Seq(
    StructField("bool", BooleanType),
    StructField("int", IntegerType),
    StructField("map", MapType(IntegerType, BooleanType)),
    StructField("list", ArrayType(IntegerType))
  ))
}
