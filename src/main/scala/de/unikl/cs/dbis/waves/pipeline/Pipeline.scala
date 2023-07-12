package de.unikl.cs.dbis.waves.pipeline

import de.unikl.cs.dbis.waves.split.Splitter

import org.apache.spark.sql.DataFrame

import de.unikl.cs.dbis.waves.partitions.PartitionTree
import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline.util.Finalizer
import de.unikl.cs.dbis.waves.sort.Sorter
import de.unikl.cs.dbis.waves.sort.NoSorter
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
  * modify the state and store its results in it. If finalization is requested
  * from the splitter, it additionally runs the Finalizer step at the end.
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

  override def doFinalize(enabled: Boolean): Splitter[Nothing] = {
      assertPrepared
      initialState = Some(DoFinalize(initialState.get) = enabled)
      this
    }

  override def finalizeEnabled: Boolean
    = { assertPrepared; DoFinalize(initialState.get) }

  override def modifySchema(enabled: Boolean): Splitter[Nothing] = {
    assertPrepared
    initialState = Some(ModifySchema(initialState.get) = enabled)
    this
  }

  override def schemaModificationsEnabled: Boolean
    = { assertPrepared; ModifySchema(initialState.get) }

  override def partition(): Unit = {
    assertPrepared

    // Run all steps sequentially followed by the sink
    var currentState = initialState.get
    for (step <- steps) {
      currentState = step(currentState)
    }
    //TODO: modify schema
    if (DoFinalize(currentState))
      currentState = Finalizer(currentState)
    val buckets = sink(currentState)

    // write metadata
    require(Shape isDefinedIn currentState)
    val shape = treeByShape(buckets, Shape(currentState))
    val tree = new PartitionTree(Schema(currentState), NoSorter, shape)
    currentState.hdfs.write(tree)
  }

  private def treeByShape(buckets: Seq[PartitionFolder], shape: AnyNode[Unit])
    = shape.map({case ((), index) => buckets(index).name})

  override def sortWith(sorter: Sorter): Splitter[Nothing]
    = throw new UnsupportedOperationException("use an appropriate step in the pipeline")
  override protected def load(context: Nothing): DataFrame = ???
}
