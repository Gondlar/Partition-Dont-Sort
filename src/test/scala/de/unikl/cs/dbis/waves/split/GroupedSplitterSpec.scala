package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.{DataFrame => DataFrameFixture}

import de.unikl.cs.dbis.waves.split.recursive.ObjectCounter
import org.apache.spark.sql.{DataFrame, Column, Row}
import org.apache.spark.sql.types.StructType
import scala.collection.mutable.{ArrayBuilder, WrappedArray}
import org.apache.spark.rdd.RDD

class GroupedSplitterSpec extends WavesSpec
    with DataFrameFixture {

    "The GroupedSplitter" should {

        "process all partitions and keep their internal order" in {
            val sets = Seq( Set(data(1), data(2))
                          , Set(data(1))
                          , Set(data(2))
                          , Set(data(2), data(3))
                          )
            val frames = for (partition <- sets) yield {
                val rdd : RDD[Row] = spark.sparkContext.parallelize(partition.toSeq)
                spark.sqlContext.createDataFrame(rdd, schema)
            }
            val sortedSets = ArrayBuilder.make[DataFrame]
            var builtSet: Seq[DataFrame] = Seq.empty
            new GroupedSplitter {

                override protected def load(context: Unit): DataFrame = df

                override protected def grouper: StructType => Column
                    = de.unikl.cs.dbis.waves.util.operators.definitionLevels

                override protected def split(df: DataFrame): Seq[DataFrame] = {
                    df.collect() should contain theSameElementsAs Seq(
                        Row(WrappedArray.make(Array(1, 1, 2)), 1),
                        Row(WrappedArray.make(Array(1, 1, 1)), 1),
                        Row(WrappedArray.make(Array(1, 0, 0)), 2),
                        Row(WrappedArray.make(Array(0, 1, 2)), 1),
                        Row(WrappedArray.make(Array(0, 1, 1)), 1),
                        Row(WrappedArray.make(Array(0, 0, 0)), 2)
                    )
                    frames
                }

                override protected def sort(bucket: DataFrame): DataFrame = {
                    sortedSets += bucket
                    super.sort(bucket)
                }

                override protected def buildTree(buckets: Seq[DataFrame]): Unit
                    = builtSet = buckets
            }.partition()
            sortedSets.result should contain theSameElementsInOrderAs (frames)
            builtSet should have length (frames.length)
            forAll((0 until frames.length)) { i =>
                builtSet(i).collect() should contain theSameElementsAs (frames(i).collect())
            }
        }
    }
}