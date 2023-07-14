package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.util.PathKey

/**
  * Manage the Metadata of a Partition, i.e., its known present and absent paths
  *
  * @param present the known present paths
  * @param absent the known absent paths
  */
class PartitionMetadata(
  private var present: Set[PathKey] = Set.empty,
  private var absent: Set[PathKey] = Set.empty,
  private var path: Vector[PartitionTreePath] = Vector.empty
) {

  /**
    * @return The paths added as present to this metadata object. Paths that
    *         are subsumed by other paths are not returned.
    */
  def getPresent: Iterable[PathKey] = present

  /**
    * @return The paths added as absent to this metadata object. Paths that
    *         are subsumed by other paths are not returned.
    */
  def getAbsent: Iterable[PathKey] = absent

  /**
    * @param key a key to check
    * @return true iff it can be deduced that key is present
    */
  def isKnownPresent(key: PathKey) = present.exists(key isPrefixOf _)

  /**
    * @param key a key to check
    * @return true iff it can be deduced that key is absent
    */
  def isKnownAbsent(key: PathKey) = absent.exists(_ isPrefixOf key)

  /**
    * @param key a key to check
    * @return true iff the presence state of key is known
    */
  def isKnown(key: PathKey) = isKnownPresent(key) || isKnownAbsent(key)

  /**
    * @param key a key to check
    * @return true iff key could still be present
    */
  def canBePresent(key: PathKey) = !isKnownAbsent(key)

  /**
    * @param key a key to check
    * @return true iff key could still be absent
    */
  def canBeAbsent(key: PathKey) = !isKnownPresent(key)

  /**
    * Add a key as absent. Adding a key is only possible if it still can be
    * absent. Any keys subsumed by the newly added key will be removed
    * 
    * @param key the key
    * @throws IllegalArgumentException if the key cannot be absent
    * @see [[canBeAbsent]]
    */
  def addAbsent(key: PathKey): Unit = {
    require(canBeAbsent(key))
    addStep(Absent)
    if (isKnownAbsent(key)) return
    addAbsentKey(key)
  }

  /**
    * Incorporate a known absent key into the set
    *
    * @param key
    */
  private def addAbsentKey(key: PathKey)
    = absent = absent.filter(p => !(key isPrefixOf p)) + key

  /**
    * Add a key as present. Adding a key is only possible if it still can be
    * present. Any keys subsumed by the newly added key will be removed
    * 
    * @param key the key
    * @throws IllegalArgumentException if the key cannot be present
    * @see [[canBePresent]]
    */
  def addPresent(key: PathKey): Unit = {
    require(canBePresent(key))
    addStep(Present)
    if (isKnownPresent(key)) return
    addPresentKey(key)
  }

  /**
    * Incoroprate a known present key into the set
    *
    * @param key
    */
  private def addPresentKey(key: PathKey)
    = present = present.filter(p => !(p isPrefixOf key)) + key

  /**
    * Add the key as present or absent according to the given path
    *
    * @param presence the path
    * @param key the key
    * @see [[addPresent]] and [[addAbsent]]
    */
  def add(presence: SplitByPresencePath, key: PathKey) = presence match {
    case Absent => addAbsent(key)
    case Present => addPresent(key)
  }

  /**
    * You only need to call this method if the step does not add metadata, the
    * other methods automatically add steps to the path
    * @param step the step to add to the path
    */
  def addStep(step: PartitionTreePath) = path = path :+ step

  /**
    * @return true iff this Metadata belongs to the Bucket of a Spill Node
    */
  def isSpillBucket = path.lastOption.map(_ == Rest).getOrElse(false)

  /**
    * @return true iff this Metadata belongs to the root of a partition tree,
    *         i.e., whether its path is empty
    */
  def isRoot = path.isEmpty

  /**
    * @return Get the path to the Node this metadata refers to
    */
  def getPath: Seq[PartitionTreePath] = path

  /**
    * Create an independant copy of this Metadata object.
    * @return the copy
    */
  override def clone = new PartitionMetadata(present, absent, path)

  override def equals(obj: Any): Boolean = obj match {
    case other: PartitionMetadata
      => other.absent == absent && other.present == present && other.path == path
    case _ => false
  }

  /**
    * Concatenate the metadata objects, i.e., the result reflects the metadata
    * as if `rhs` was the metadata of the subtree rooted in the node this
    * metadata object represents.
    * 
    * This, paths known to be present or absent in either metadata object will
    * be known as such in the result and the paths are concatenated accordingly.
    *
    * @param rhs the metadata object to append
    */
  def ++=(rhs: PartitionMetadata) = {
    require(!isSpillBucket)

    for(absent <- rhs.getAbsent) addAbsentKey(absent)
    for(present <- rhs.getPresent) addPresentKey(present)
    path ++= rhs.path
  }

  /**
    * Concatenate two metadata objects
    * 
    * Works identical to [[++=]], but both objects stay unchanged.
    *
    * @param rhs the other metadata object to append
    * @see [[++=]]
    */
  def ++(rhs: PartitionMetadata) = {
    val result = clone()
    result ++= rhs
    result
  }
}

object PartitionMetadata {
  def apply() = new PartitionMetadata()
  def apply(present: Seq[PathKey], absent: Seq[PathKey], path: Seq[PartitionTreePath])
    = new PartitionMetadata(present.toSet, absent.toSet, path.toVector)
}
