package de.unikl.cs.dbis.waves.pipeline

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import org.apache.spark.sql.types.StructType

final case class PipelineState(
  val data : DataFrame,
  val path : String,
  private val metadata : Map[String, _] = Map.empty
) {

  def hdfs = PartitionTreeHDFSInterface(data.sparkSession, path)

  def getMetadata[T](key: String) : Option[T]
    = metadata.get(key).map(_.asInstanceOf[T])
  
  def setMetadata[T](key: String, payload: T)
    = copy(metadata = (metadata + ((key, payload))))
}

abstract class StateValue[T](key: String) {
  def get(state: PipelineState): Option[T] = state.getMetadata[T](key)
  def apply(state: PipelineState): T = get(state).get
  def isDefined(state: PipelineState) = state.getMetadata[T](key).isDefined
  def update(state: PipelineState, value: T) = state.setMetadata(key, value)
}

abstract class StateValueWithDefault[T](key: String, default: T)
extends StateValue[T](key) {
  override def apply(state: PipelineState): T = get(state).getOrElse(default)
}

object Shape extends StateValue[AnyNode[Unit]]("state")
object Buckets extends StateValue[Seq[DataFrame]]("buckets")
object GlobalSortorder extends StateValue[Seq[Column]]("globalSortOrder")
object BucketSortorders extends StateValue[Seq[Seq[Column]]]("bucketSortOrders")
object ModifySchema extends StateValueWithDefault[Boolean]("modifySchema", false)
object DoFinalize extends StateValueWithDefault[Boolean]("doFinalize", true)
object Schema extends StateValue[StructType]("schema") {
  override def apply(state: PipelineState): StructType = get(state).getOrElse(state.data.schema)
}