package de.unikl.cs.dbis.waves.pipeline

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.util.StructuralMetadata
import org.apache.spark.sql.types.StructType

/**
  * This class holds the state between steps in a Pipeline.
  * 
  * It is immutable.
  *
  * @param data the data frame to read
  * @param path the path to write to
  * @param metadata intermediate results of arbitrary type, identified by key
  */
final case class PipelineState(
  val data : DataFrame,
  val path : String,
  private val metadata : Map[String, _] = Map.empty
) {

  /**
    * @return The HDFS interface for the state's path
    */
  def hdfs = PartitionTreeHDFSInterface(data.sparkSession, path)

  /**
    * Type-safe access to metadata of a known type
    *
    * @param key the key for the data to be accessed
    * @return the metadata stored in this state or Nothing if the key is undefined
    */
  def getMetadata[T](key: String) : Option[T]
    = metadata.get(key).map(_.asInstanceOf[T])
  
  /**
    * Type-safe way of storing metadata at a key
    *
    * @param key the key for the data to be stored
    * @param payload the data
    * @return a new PipelineState with the requested changes
    */
  def setMetadata[T](key: String, payload: T)
    = copy(metadata = (metadata + ((key, payload))))
}

/**
  * Abstract class for wrapping access operations to specific keys in a state's metadata.
  * 
  * The syntax for this is a bit unsual but prevents reimplementing the same logic
  * for every key while remaining type-safe. Given a StateValue "Foo" and a state:
  *   - Foo(state) // returns the value of Foo in state or fails if it is unset
  *   - Foo.isDefined(state) // checks whether Foo is defined in state
  *   - val newState = Foo(state) = v // create a new state where Foo is v
  *
  * @param key The key this Value accesses
  */
abstract class StateValue[T](key: String) {

  /**
    * Return this value for a state as an option
    *  
    * @param state the state
    * @return the option containing the result
    */
  def get(state: PipelineState): Option[T] = state.getMetadata[T](key)

  /**
    * Return this value for a state or fail if it is unset
    *
    * @param state the state
    * @return the value
    */
  def apply(state: PipelineState): T = get(state).get

  /**
    * Check whether this value is defined in a state
    *
    * @param state the state
    * @return true iff the value is defined
    */
  def isDefinedIn(state: PipelineState) = state.getMetadata[T](key).isDefined

  /**
    * Create a new state where this value is set to a given value
    *
    * @param state the initial state
    * @param value the new value to set
    * @return the new state where the value has been set
    */
  def update(state: PipelineState, value: T) = state.setMetadata(key, value)
}

/**
  * Subclass of StateValue for values with a fixed default value. The default
  * will be returned if the value is unset in the state, i.e., apply will never
  * fail.
  * 
  * get and isDefined still operate on the actual state and ignore the default
  * to allow differing behaviour in special cases.
  *
  * @param key the key this value accesses
  * @param default its default value
  */
abstract class StateValueWithDefault[T](key: String, default: T)
extends StateValue[T](key) {
  /**
    * Return this value for a state or the default if it is unset
    *
    * @param state the state
    * @return the value
    */
  override def apply(state: PipelineState): T = get(state).getOrElse(default)
}

/**
  * Stores the shape of the partition tree, i.e., its buckets are not linked to
  * any value yet. This must be set by the end of every pipeline.
  */
object Shape extends StateValue[AnyNode[Unit]]("state")

/**
  * Stores the buckets as a List of dataframes. The positions in the list
  * correspond to the Buckets in the shape from left to right as returned by
  * its buckets method.
  * 
  * Note: Modelling Buckets as Spark Partitions is likely more efficient than
  * this, but is not yet implemented and incompatible with schema modifications
  * without major patches to Spark.
  */
object Buckets extends StateValue[Seq[DataFrame]]("buckets")

/**
  * Stores a sequence of Spark Columns by which all Buckets are to be sorted.
  * This assumes a recursive sorting algorithm such as Lexicographic Sorting or
  * Gray Orders.
  * 
  * If both this and BucketSortorders are set, the Bucket order takes precedence
  */
object GlobalSortorder extends StateValue[Seq[Column]]("globalSortOrder")

/**
  * Stores a List of Sortorders. The positions in the list correspond to the
  * Buckets in the Shape from left to right as returned by its buckets method.
  * Each order is then to be used for sorting that Bucket.
  * 
  * If both this and GlobalSortorder are set, this one takes precedence
  */
object BucketSortorders extends StateValue[Seq[Seq[Column]]]("bucketSortOrders")

/**
  * Stores whether we want to modify the schema when writing PartitionFolders.
  * The default is false.
  */
object ModifySchema extends StateValueWithDefault[Boolean]("modifySchema", false)

/**
  * Stores whether we want to merge all Parquet files in a Bucket into one when
  * writing PartitionFolders. The default is true.
  */
object DoFinalize extends StateValueWithDefault[Boolean]("doFinalize", true)

/**
  * Stores metadata known about all rows in the dataset. This may be useful for
  * repartitioning an existing tree or working with data that results from a
  * query whose filters imply information on the data. The default is empty.
  */
object KnownMetadata extends StateValueWithDefault[PartitionMetadata]("knownMetadata", PartitionMetadata())

/**
  * Stores the schema of the input data. This is useful in cases where we know a
  * more specific schema than the one Spark keep in the DataFrame, e.g., because
  * it forcibly sets all Schema Nodes to optional.
  * 
  * By default, we return the DataFrame's schema.
  */
object Schema extends StateValue[StructType]("schema") {
  /**
    * Return the schema based on the state. If no state is set in the state, we
    * fall back to the schema from the DataFrame in the state.
    *
    * @param state the state
    * @return the schema
    */
  override def apply(state: PipelineState): StructType = get(state).getOrElse(state.data.schema)
}

/**
  * Stores the VersionTree of the input data.
  */
object StructureMetadata extends StateValue[StructuralMetadata]("structureMetadata")

/**
  * Stores the number of Buckets
  */
object NumBuckets extends StateValue[Int]("numBuckets")
