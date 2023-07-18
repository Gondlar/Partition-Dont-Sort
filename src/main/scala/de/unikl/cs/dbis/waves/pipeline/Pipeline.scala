package de.unikl.cs.dbis.waves.pipeline

import de.unikl.cs.dbis.waves.split.Splitter

import org.apache.spark.sql.DataFrame

import de.unikl.cs.dbis.waves.partitions.PartitionTree
import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline.util.Finalizer
import de.unikl.cs.dbis.waves.pipeline.util.SchemaModifier
import de.unikl.cs.dbis.waves.sort.{Sorter,NoSorter,LexicographicSorter}
import de.unikl.cs.dbis.waves.pipeline.sort._
import de.unikl.cs.dbis.waves.util.PartitionFolder

/**
  * A splitter implemented in terms of a sequence of loosely coupled steps.
  * 
  * This design allows us to switch various aspects of the ingestion pipeline
  * without having to reimplement everything from scratch. Not all sequences of
  * steps are valid, take care when you choose the steps. The final step, i.e.,
  * the PipelineSink, will then write each bucket to disk. Consult the
  * documentation of the sink to see which state variables should be set.
  * Additionally, Shape must be set at the end of the pipeleine.
  * 
  * The initial configuration of the splitter is written to a [[PipelineState]]
  * which is then iteratively passed to each pipeline step. Each step can then
  * modify the state and store its results in it. If schema modifications are
  * enabled, the SchemaModifier step is applied before writing the result. If
  * finalization is requested, it additionally runs the Finalizer step at the end.
  *
  * @param steps the steps to be taken in the pipeline
  * @param sink the method by which the buckets get written to disk
  */
class Pipeline(
  steps: Seq[PipelineStep],
  sink: PipelineSink
) extends Splitter[Nothing] {

  var initialState: Option[PipelineState] = None

  override def prepare(df: DataFrame, path: String): Splitter[Nothing]
    = { initialState = Some(PipelineState(df, path)); this }

  override def isPrepared: Boolean = initialState.isDefined

  override def getPath: String
    = { assertPrepared; initialState.get.path }

  private var finalizeBuckets = true

  override def doFinalize(enabled: Boolean): Splitter[Nothing]
    = { finalizeBuckets = enabled; this }

  override def finalizeEnabled: Boolean = finalizeBuckets

  private var schemaModifications = false

  override def modifySchema(enabled: Boolean): Splitter[Nothing]
    = { schemaModifications = enabled; this }

  override def schemaModificationsEnabled: Boolean
    = schemaModifications

  override def partition(): Unit = {
    assertPrepared

    // Prepare initial state
    var currentState = initialState.get
    currentState = DoFinalize(currentState) = finalizeBuckets
    currentState = ModifySchema(currentState) = schemaModifications

    // Run all steps sequentially followed by the sink
    for (step <- steps) {
      currentState = step(currentState)
    }
    if (ModifySchema(currentState))
      currentState = SchemaModifier(currentState)
    if (DoFinalize(currentState))
      currentState = Finalizer(currentState)
    val (finalState, buckets) = sink(currentState)

    // write metadata
    require(Shape isDefinedIn finalState)
    val shape = treeByShape(buckets, Shape(finalState))
    val tree = new PartitionTree(Schema(finalState), NoSorter, shape)
    finalState.hdfs.write(tree)
  }

  private def treeByShape(buckets: Seq[PartitionFolder], shape: AnyNode[Unit])
    = shape.map({case ((), index) => buckets(index).name})

  override def sortWith(sorter: Sorter): Splitter[Nothing]
    = throw new UnsupportedOperationException("use an appropriate step in the pipeline")
  override protected def load(context: Nothing): DataFrame = ???
}

object Pipeline {
  def mapLegacySorter(sorter: Sorter) = sorter match {
    case NoSorter => Seq.empty
    case LexicographicSorter => Seq(LocalOrder(ExactCardinalities), DataframeSorter)
  }
}