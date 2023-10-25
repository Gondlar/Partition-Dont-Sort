package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.nested.schemas._
import de.unikl.cs.dbis.waves.util.nested.DataTypeVisitor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{
  when, min, max, approx_count_distinct, count, array, struct
}
import org.apache.spark.sql.types.{StructType, DataType, ArrayType, MapType, BooleanType}

import de.unikl.cs.dbis.waves.util.{VersionTree, Leaf, Versions}
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.nested.SingleResult
import de.unikl.cs.dbis.waves.util.ColumnValue
import de.unikl.cs.dbis.waves.util.UniformColumnMetadata
import de.unikl.cs.dbis.waves.util.BooleanColumnMetadata
import de.unikl.cs.dbis.waves.util.operators.TempColumn
import de.unikl.cs.dbis.waves.util.operators.collect_set_with_count
import de.unikl.cs.dbis.waves.util.ColumnMetadata
import de.unikl.cs.dbis.waves.util.TotalFingerprint

/**
  * Set the StructureMetadata field of the Pipeline State to a VersionTree
  * calculated from the state's data.
  */
object CalculateTotalFingerprint extends PipelineStep with NoPrerequisites {

  override def run(state: PipelineState): PipelineState = {
    val tree = fromDataFrame(state.data, Schema(state))
    StructureMetadata(state) = tree
  }

  /**
    * Construct a VersionTree which holds the metadata of a given DataFrame and
    * schema. This method will perform two scans on the DataFrame
    *
    * @param df the DataFrame to process
    * @param schema the schema of the df
    * @return the constructed VersionTree
    */
  def fromDataFrame(df: DataFrame, schema: StructType): TotalFingerprint = {
    val sortedLeafs = schema.leafPaths.sortBy(_.toString())
    val sortedOptionalNodes = schema.paths.sortBy(_.toString())

    val fingerprint = collect_set_with_count[IndexedSeq[Boolean]].apply(array(sortedOptionalNodes.map(_.toCol.isNotNull):_*))
    val aggregates = for {
      leafpath <- sortedLeafs
      leaf <- leafpath.retrieveFrom(schema).toSeq
      feature <- columnsForLeaf(leafpath, leaf)
     } yield feature
    val info = df.agg(fingerprint, aggregates:_*).collect()(0)

    val nextPosition = (1 to info.size).iterator
    val leafs = for {
      leafpath <- sortedLeafs
      leaf <- leafpath.retrieveFrom(schema).toSeq
    } yield {
      val foo: Option[ColumnMetadata] = leaf match {
        case BooleanType => {
          val falseIndex = nextPosition.next()
          val trueIndex = nextPosition.next()
          BooleanColumnMetadata.fromCounts(info.getLong(falseIndex), info.getLong(trueIndex))
        }
        case _ => {
          val minIndex = nextPosition.next()
          val maxIndex = nextPosition.next()
          val distinctIndex = nextPosition.next()
          for {
            min <- ColumnValue.fromRow(info, minIndex)
            max <- ColumnValue.fromRow(info, maxIndex)
          } yield {
            val distinct = info.getLong(distinctIndex)
            UniformColumnMetadata(min, max, distinct)
          }
        }
      }
      foo
    }
    TotalFingerprint(
      sortedOptionalNodes.map(_.toString()).toIndexedSeq,
      parseFingerprints(info.getSeq[Row](0), sortedOptionalNodes.size),
      leafs.toIndexedSeq,
      sortedLeafs.map(_.toString()).toIndexedSeq
    )
  }

  def columnsForLeaf(path: PathKey, tpe: DataType): Seq[Column] = tpe match {
    case BooleanType => Seq(count(when(!path.toCol, 1)),count(when(path.toCol, 1)))
    case _ => Seq(min(path.toCol), max(path.toCol), approx_count_distinct(path.toCol))
  }

  private def parseFingerprints(versions: Seq[Row], structWidth: Int) = {
      val fingerprints = for (version <- versions) yield {
        val signature = version.getSeq[Boolean](0).toIndexedSeq
        val count = version.getLong(1)
        (signature, count)
      }
      assert(!(fingerprints.map(_._1) contains null))
      if (fingerprints.isEmpty) {
        Seq((IndexedSeq.fill(structWidth)(false), 0L))
      } else fingerprints
    }
}
