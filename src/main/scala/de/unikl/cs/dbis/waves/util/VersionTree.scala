package de.unikl.cs.dbis.waves.util

import Math.max

/**
  * A datastructure for keeping structural Metadata which assumes independance
  * between individual structs
  */
sealed trait VersionTree extends StructuralMetadata {

  override def setMetadata(path: Option[PathKey], metadata: ColumnMetadata): Either[String,VersionTree]
  def splitBy(path: PathKey) : Either[String,(VersionTree, VersionTree)]
  override def splitBy(leaf: Option[PathKey], quantile: Double): Either[String,(VersionTree, VersionTree)]

  final def gini: Double = {
    val (count, g, dataColumnGini) = gini(1)
    count - g + dataColumnGini
  }

  protected[util] def gini(baseProbability: Double): (Int, Double, Double)
}

final case class Leaf(
  metadata: Option[ColumnMetadata] = None
) extends VersionTree {

  override def isCertain(path: PathKey): Boolean = true

  //this looks wrong but it's not: paths are non-empty, so we know path refers to something non-existant
  override def isLeaf(path: PathKey, withMetadata: Boolean): Boolean = false

  override def setMetadata(path: Option[PathKey], metadata: ColumnMetadata): Either[String,VersionTree]
    = if (path.isDefined) Left("path does not exist") else Right(copy(metadata = Some(metadata)))

  override def absoluteProbability(path: Option[PathKey]): Double
    = if (path.isDefined) 0 else 1
  
  override def separatorForLeaf(path: Option[PathKey], quantile: Double = .5): Either[String,(ColumnValue, Double)]
    = path match {
      case None => metadata.toRight("no metadata available").map { m =>
        val separator = m.separator(quantile)
        val probability = m.probability(separator).getOrElse(quantile)
        (separator, probability)
      }
      case Some(value) => Left("path is not a leaf")
    }
  
  override def splitBy(path: PathKey): Either[String,(VersionTree, VersionTree)]
    = Left("path does not exist")

  override def splitBy(leaf: Option[PathKey], quantile: Double): Either[String,(VersionTree, VersionTree)] = {
    if (quantile <=  0 || 1 <= quantile)
      return Left("0 < quantile < 1 must hold")
    for {
      data <- metadata.toRight("no column metadata found")
      tuple <- data.split(quantile)
    } yield (copy(metadata = Some(tuple._1)), copy(metadata = Some(tuple._2)))
  }

  override protected[util] def gini(baseProbability: Double): (Int, Double, Double) = {
    val dataColumGini = metadata
      .filter(_ => baseProbability > 0)
      .map(_.gini)
      .getOrElse(0d)
    (1, baseProbability * baseProbability, dataColumGini)
  }
}

object Leaf {
  val empty = Leaf()
}

final case class Versions(
  childNames: IndexedSeq[String],
  children: IndexedSeq[VersionTree],
  versions: Seq[(IndexedSeq[Boolean], Double)]
) extends VersionTree {
  assert(childNames.size == children.size)                // child names correspond to children
  assert(childNames == childNames.sorted)                 // names are sorted
  assert(versions.nonEmpty)                               // there is at least one version
  assert(versions.forall(_._1.size == childNames.size))   // version signatures correspond to children
  assert(versions.map(_._1).toSet.size == versions.size)  // versions appear exactly once
  assert(versions.forall(_._2 > 0))                       // versions have probability higher than 0
  assert(approx(versions.map(_._2).sum, 1))               // all probablities sum to 1

  private def approx(lhs: Double, rhs: Double, precision: Double = 0.0000000001)
    = (lhs - rhs).abs < precision

  private def childIndex(name: String): Option[Int] = {
    // binary search child
    var low = 0
    var high = childNames.size-1
    while (low < high) {
      val middle = (low + high)/2
      childNames(middle).compareTo(name) match {
        case 0 => return Some(middle)
        case c if c < 0 => low = max(middle, low+1)
        case c if c > 0 => high = middle
      }
    }
    if (childNames(low) == name) Some(low) else None
  }

  private def conditionalProbability(childIndex: Int)
    = versions.filter(_._1(childIndex)).map(_._2).sum
  
  override def isCertain(path: PathKey): Boolean
    = childIndex(path.head).map { index => 
      conditionalProbability(index) match {
        case 0 => true
        case 1 => !path.isNested || children(index).isCertain(path.tail)
        case _ => false
      }
    }.getOrElse(true)

  override def isLeaf(path: PathKey, withMetadata: Boolean): Boolean = {
    childIndex(path.head) match {
      case None => false
      case Some(index) => children(index) match {
        case Leaf(metadata) => !path.isNested && (!withMetadata || metadata.isDefined)
        case struct@Versions(_, _, _) => path.isNested && struct.isLeaf(path.tail, withMetadata)
      }
    }
  }

  override def setMetadata(path: Option[PathKey], metadata: ColumnMetadata): Either[String,VersionTree]
    = for {
      existingPath <- path.toRight("path is an inner node")
      index <- childIndex(existingPath.head).toRight("path does not exist")
      newChild <- children(index).setMetadata(path.tail, metadata)
    } yield copy(children = children.updated(index, newChild))
  
  override def absoluteProbability(path: Option[PathKey]): Double
    = path match {
      case None => 1
      case Some(pathToChild) => childIndex(pathToChild.head) match {
        case None => 0
        case Some(index) => conditionalProbability(index) * children(index).absoluteProbability(path.tail)
      }
    }

  override def separatorForLeaf(path: Option[PathKey], quantile: Double = .5): Either[String,(ColumnValue, Double)]
    = for {
      existingPath <- path.toRight("path is not a leaf")
      index <- childIndex(existingPath.head).toRight("path not found")
      separator <- children(index).separatorForLeaf(path.tail, quantile)
    } yield separator

  override def splitBy(path: PathKey): Either[String,(VersionTree, VersionTree)]
    = for {
      index <- childIndex(path.head).toRight("path does not exist")
    } yield {
      val (present, absent) = versions.partition(_._1(index))
      if (present.isEmpty) return Left("cannot split on absent paths")
      if (path.isNested) {
        val either = children(index).splitBy(path.tail)
        if (either.isLeft) return either
        val stillPresentFraction = 1 - children(index).absoluteProbability(path.tail)
        val (childAbsent, childPresent) = either.right.get
        (
          copy(children = children.updated(index, childAbsent), versions = normalize(scale(present, stillPresentFraction) ++ absent)),
          copy(children = children.updated(index, childPresent), versions = normalize(present))
        )
      } else {
        if (absent.isEmpty) return Left("cannot split at certain locations")
        (copy(versions = normalize(absent)), copy(versions = normalize(present)))
      }
    }
    
  override def splitBy(path: Option[PathKey], quantile: Double): Either[String,(VersionTree, VersionTree)] = {
    if (quantile <=  0 || 1 <= quantile)
      return Left("0 < quantile < 1 must hold")
    for {
      existingPath <- path.toRight("path is not a leaf")
      index <- childIndex(existingPath.head).toRight("invalid path")
      child = children(index)
      newChildren <- child.splitBy(path.tail, quantile)
    } yield {
      val (present, absent) = versions.partition(_._1(index))
      if (present.isEmpty) return Left("cannot split an absent path")
      val stillPresentFraction = 1 - child.absoluteProbability(path.tail) * quantile
      (
        copy(children = children.updated(index, newChildren._1), versions = normalize(present)),
        copy(children = children.updated(index, newChildren._2), versions = normalize(scale(present, stillPresentFraction)++absent))
      )
    }
  }

  private def scale(versions: Seq[(IndexedSeq[Boolean], Double)], factor: Double)
    = versions.map{case (version, probability) => (version, probability*factor)}
  
  private def normalize(versions: Seq[(IndexedSeq[Boolean], Double)]) = {
    val total = versions.map(_._2).sum
    versions.map{case (version, probability) => (version, probability/total)}
  }

  override protected[util] def gini(baseProbability: Double): (Int, Double, Double) = {
    var count = 0
    var sum = 0.0
    var dataColumnGini = 0.0
    for ((_, conditional, child) <- childIterator) {
      // sum all squared probabilities if this node is present
      val presentProbability = conditional * baseProbability
      val (childCount, childGini, childDataGini) = child.gini(presentProbability)
      sum += childGini
      dataColumnGini += childDataGini
      
      // for each leaf under this child, add the absent probability once
      val absentProbability = (1-conditional) * baseProbability
      sum += childCount * (absentProbability) * (absentProbability)
      count += childCount
    }
    (count, sum, dataColumnGini)
  }

  def childIterator = for {
    ((name, child), index) <- childNames.iterator.zip(children.iterator).zipWithIndex
  } yield (name, conditionalProbability(index), child)

  override def equals(other: Any): Boolean = other match {
    case Versions(otherNames, otherChildren, otherVersions) => {
      (otherNames sameElements childNames) && (otherChildren sameElements children) && otherVersions.toSet == versions.toSet
    }
    case _ => false
  }
}
