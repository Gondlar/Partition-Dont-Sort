package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.pipeline.sample.{Sampler, NullSampler}
import de.unikl.cs.dbis.waves.util.nested.schemas._
import de.unikl.cs.dbis.waves.util.nested.DataTypeVisitor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{
  when, min, max, approx_count_distinct, count, array, struct
}
import org.apache.spark.sql.types.{StructType, DataType, ArrayType, MapType, BooleanType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.execution.QueryExecution

import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.ColumnValue
import de.unikl.cs.dbis.waves.util.UniformColumnMetadata
import de.unikl.cs.dbis.waves.util.BooleanColumnMetadata
import de.unikl.cs.dbis.waves.util.ColumnMetadata
import de.unikl.cs.dbis.waves.util.TotalFingerprint
import de.unikl.cs.dbis.waves.util.Logger

import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.UUID

/**
  * Set the StructureMetadata field of the Pipeline State to a VersionTree
  * calculated from the state's data.
  */
case class CalculateTotalFingerprint(
  sampler: Sampler = NullSampler,
  keepPercent: Option[Double] = None
) extends PipelineStep with NoPrerequisites {
  assert(keepPercent.forall(p => p > 0 && p < 1))

  import CalculateTotalFingerprint._

  override def run(state: PipelineState): PipelineState = {
    Logger.log("parameter-sampler", sampler.explain)
    Logger.log("parameter-fingerprintPruning", keepPercent.getOrElse("Off"))
    val tree = fromDataFrame(sampler(state.data), Schema(state), keepPercent)
    Logger.log("metadata-fingerprintCount", tree.fingerprints.length)
    StructureMetadata(state) = tree
  }
}

object CalculateTotalFingerprint {

  /**
    * Construct a VersionTree which holds the metadata of a given DataFrame and
    * schema. This method will perform two scans on the DataFrame
    *
    * @param df the DataFrame to process
    * @param schema the schema of the df
    * @return the constructed VersionTree
    */
  def fromDataFrame(df: DataFrame, schema: StructType, keepPercent: Option[Double] = None): TotalFingerprint = {
    val sortedLeafs = schema.leafPaths.sortBy(_.toString())
    val sortedOptionalNodes = schema.paths.sortBy(_.toString())

    val aggregates = for {
      leafpath <- sortedLeafs
      leaf <- leafpath.retrieveFrom(schema).toSeq
      feature <- columnsForLeaf(leafpath, leaf)
    } yield feature
    val fingerprintColumn = array(sortedOptionalNodes.map(_.toCol.isNotNull):_*) as "fingerprint"
    val (observedDf, future) = observeAggregates(df, aggregates)
    val rawFingerprints = observedDf.groupBy(fingerprintColumn).count().collect()
    val leafs = parseLeafMetadata(sortedLeafs, schema, Await.result(future, Duration(1L, "min")))
    
    val fingerprints = parseFingerprints(rawFingerprints, sortedOptionalNodes.size)
    val prunedFingerprints = keepPercent.map(p => pruneFingerprints(fingerprints, p)).getOrElse(fingerprints)
    TotalFingerprint(
      sortedOptionalNodes.map(_.toString()).toIndexedSeq,
      prunedFingerprints,
      leafs.toIndexedSeq,
      sortedLeafs.map(_.toString()).toIndexedSeq
    )
  }

  private def columnsForLeaf(path: PathKey, tpe: DataType): Seq[Column] = tpe match {
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

  private def pruneFingerprints(fingerprints: Seq[(IndexedSeq[Boolean], Long)], keepPercent: Double) = {
    val sortedFingerprints = fingerprints.sortBy(-_._2)
    val threshold = (sortedFingerprints.iterator.map(_._2).sum * keepPercent).ceil.toLong
    var sum = 0L
    sortedFingerprints.takeWhile({ case (_, count) =>
      sum += count
      sum <= threshold
    })
  }

  private def parseLeafMetadata(sortedLeafs: Iterable[PathKey], schema: StructType, row: Row) = {
    val nextPosition = (0 until row.size).iterator
    val result = for {
      leafpath <- sortedLeafs
      leaf <- leafpath.retrieveFrom(schema).toSeq
    } yield {
      val foo: Option[ColumnMetadata] = leaf match {
        case BooleanType => {
          val falseIndex = nextPosition.next()
          val trueIndex = nextPosition.next()
          BooleanColumnMetadata.fromCounts(row.getLong(falseIndex), row.getLong(trueIndex))
        }
        case _ => {
          val minIndex = nextPosition.next()
          val maxIndex = nextPosition.next()
          val distinctIndex = nextPosition.next()
          for {
            min <- ColumnValue.fromRow(row, minIndex)
            max <- ColumnValue.fromRow(row, maxIndex)
          } yield {
            val distinct = row.getLong(distinctIndex)
            UniformColumnMetadata(min, max, distinct)
          }
        }
      }
      foo
    }
    result.toIndexedSeq
  }

  private def observeAggregates(df: DataFrame, aggregates: Seq[Column]) = {
    val manager = df.sparkSession.listenerManager
    val name = "leafs-" + UUID.randomUUID()
    val listener = new LeafMetadataListener(name, manager.unregister)
    manager.register(listener)
    (df.observe(name, aggregates.head, aggregates.tail:_*), listener.future)
  }

  class LeafMetadataListener(myName: String, unregisterAction: LeafMetadataListener => Unit) extends QueryExecutionListener {
    private val promise = Promise[Row]()

    def future = promise.future

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit
      = qe.observedMetrics.get(myName) match {
        case Some(value) => {
          promise.success(value)
          unregisterAction(this)
        }
        case None => ()
      }

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit
      = if (qe.observedMetrics.contains(myName)) {
        promise.failure(exception)
        unregisterAction(this)
      }
  }
}
