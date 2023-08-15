package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.PartitionTreePath
import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode
import de.unikl.cs.dbis.waves.partitions.Bucket

import scala.collection.mutable.PriorityQueue
import org.apache.spark.sql.DataFrame

/**
  * An abstract base class for recursive splitting operations as Pipeline steps.
  * 
  * This class assumes that a partitioning is derived by recursively splitting
  * a dataset and recording the respective actions in the partition tree. Splits
  * are not necesarily performed in the order in which they are discovered, but
  * based on a priority.
  * 
  * Finally, the resulting shape is set in the Pipeline State
  */
abstract class Recursive[RS <: RecursionState] extends PipelineStep {

  protected implicit val ord = Ordering.by[RS, Double](_.priority)

  /**
    * Calculate the initial recursion state based on a given Pipeline State
    *
    * @param state the pipeline state
    * @return the recursion state
    */
  protected def initialRecursionState(state: PipelineState): RS

  /**
    * Check whether we want to do another recursion step. If yes, doRecursionStep
    * will be called with the same parameter followed by another call to this
    * method.
    * 
    * Calls to this method will be omitted if no recursion states are available.
    * In this case, the recusrion immediately terminates.
    *
    * @param recState The next RecursionState - if any
    * @return true iff we want to run another step
    */
  protected def checkRecursion(recState: RS): Boolean

  /**
    * Perform the next step in the recursion on the given recursion state.
    * 
    * Any states returned will be added to the queue of states to be processed
    *
    * @param recState the state of the step
    * @param df the DataFrame being split
    * @return the recursive steps below the one just undertaken
    */
  protected def doRecursionStep(recState: RS, df: DataFrame): Seq[RS]

  override def run(state: PipelineState): PipelineState = {
    val queue = PriorityQueue(initialRecursionState(state))
    var currentShape: AnyNode[DataFrame] = Bucket(state.data)
    while (queue.nonEmpty && checkRecursion(queue.head)) {
      val step = queue.dequeue
      val df = currentShape.find(step.path).get.asInstanceOf[Bucket[DataFrame]].data
      queue.enqueue(doRecursionStep(step, df):_*)
      currentShape = currentShape.replace(step.path, step.splitShape(df))
    }
    val withShape = Shape(state) = currentShape.shape
    Buckets(withShape) = currentShape.buckets.map(_.data)
  }
}

/**
  * The state between recursion steps
  * 
  * None of the methods of this trait should require long computations or depend
  * on global state
  */
trait RecursionState {

  /**
    * @return The priority of this step
    */
  def priority: Double

  /**
    * @return the path to the bucket which is split by this step
    */
  def path: Seq[PartitionTreePath]

  /**
    * @return the shape this split creates from the bucket
    */
  def splitShape(df: DataFrame): AnyNode[DataFrame]
}
