package de.unikl.cs.dbis.waves.util

import Math.max

final case class TotalFingerprint(
  names: IndexedSeq[String],
  fingerprints: Seq[(IndexedSeq[Boolean], Long)],
  leafMetadata : IndexedSeq[Option[ColumnMetadata]],
  leafs: IndexedSeq[String],
  sums: IndexedSeq[Long],
  total: Long
) extends StructuralMetadata {
  assert(names == names.sorted)                          // Paths to all nodes are sorted
  assert(fingerprints.forall(_._1.size == names.size))   // All Fingerprints have the correct sze
  assert(leafs == leafs.sorted)                          // Paths to all leafs are sorted
  assert(leafs.forall(names.contains(_)))                // All leafs are contained in all nodes
  assert(leafs.size == leafMetadata.size)                // We have as much Metadata as we have leafs
  assert(sums.size == names.size)                        // We have a sum for each node

  private def indexIn(name: String, list: IndexedSeq[String]): Option[Int] = {
    // binary search child
    var low = 0
    var high = list.size-1
    while (low < high) {
      val middle = (low + high)/2
      list(middle).compareTo(name) match {
        case 0 => return Some(middle)
        case c if c < 0 => low = max(middle, low+1)
        case c if c > 0 => high = middle
      }
    }
    if (list(low) == name) Some(low) else None
  }

  override def isCertain(path: PathKey): Boolean = {
    val index = indexIn(path.toString(), names) match {
      case None => return true
      case Some(v) => v
    }
    val parentFilter: (IndexedSeq[Boolean] => Boolean) = path.parent match {
      case None => candidate => true
      case Some(parent) => {
        val parentIndex = indexIn(parent.toString(), names).get
        candidate => candidate(parentIndex)
      }
    }
    // we could recursively implement the fold and short-circuite once the first 
    // differing value appears
    val (hasTrue, hasFalse) = fingerprints.iterator.filter(c => parentFilter(c._1)).map(_._1(index)).foldLeft((false, false))({ case ((wasTrue, wasFalse), next) =>
      if (next) (true, wasFalse) else (wasTrue, true)
    })
    !(hasTrue && hasFalse)
  }

  override def isLeaf(path: PathKey, withMetadata: Boolean): Boolean
    = indexIn(path.toString(), leafs).exists(!withMetadata || leafMetadata(_).isDefined)

  override def setMetadata(path: Option[PathKey], metadata: ColumnMetadata): Either[String,TotalFingerprint]
    = for {
      existingPath <- path.toRight("root cannot be a leaf")
      index <- indexIn(existingPath.toString(), leafs).toRight("leaf does not exist")
    } yield copy(leafMetadata = leafMetadata.updated(index, Some(metadata)))

  override def absoluteProbability(path: Option[PathKey]): Double
    = path match {
      case None => 1
      case Some(pathToChild) => indexIn(pathToChild.toString(), names) match {
        case None => 0
        case Some(index) => absoluteProbability(index)
      }
    }

  private def absoluteProbability(index: Int)
    = sums(index).toDouble/total

  override def separatorForLeaf(path: Option[PathKey], quantile: Double): Either[String,(ColumnValue, Double)]
    = for {
      existingPath <- path.toRight("path is not a leaf")
      index <- indexIn(existingPath.toString(), leafs).toRight("path not found")
      metadata <- leafMetadata(index).toRight("no metadata for given leaf")
    } yield {
      val separator = metadata.separator(quantile)
      val probablility = metadata.probability(separator).getOrElse(quantile)
      (separator, probablility)
    }

  override def splitBy(path: PathKey): Either[String,(TotalFingerprint, TotalFingerprint)]
    = for {
      index <- indexIn(path.toString(), names).toRight("path does not exist")
    } yield {
      if (isCertain(path)) return Left("cannot split on paths that are certain")
      val (present, absent) = fingerprints.partition(_._1(index))
      if (absent.isEmpty || present.isEmpty) return Left("cannot split on paths that are certain")
      val presentSums = TotalFingerprint.fingerprintSums(present)
      val presentTotal = TotalFingerprint.fingerprintTotal(present)
      val absentSums = for (i <- sums.indices) yield sums(i) - presentSums(i)
      val absentTotal = total - presentTotal
      (TotalFingerprint(names, absent, leafMetadata, leafs, absentSums, absentTotal), TotalFingerprint(names, present, leafMetadata, leafs, presentSums, presentTotal))
    }

  override def splitBy(leaf: Option[PathKey], quantile: Double): Either[String,(TotalFingerprint, TotalFingerprint)] = {
    if (quantile <= 0 || quantile >= 1) return Left("0 < quantile < 1 must hold")
    for {
      existingPath <- leaf.toRight("root cannot be a leaf")
      leafIndex <- indexIn(existingPath.toString(), leafs).toRight("path does not exist")
      metadata <- leafMetadata(leafIndex).toRight("leaf has no metadata")
      columnSplits <- metadata.split(quantile)
    } yield {
      val (present, absent) = fingerprints.partition(_._1(indexIn(existingPath.toString(), names).get))
      val fraction = metadata.refinedProbability(quantile)
      val presentAndLess = present.map({ case (fingerprint, count) => (fingerprint, (count*fraction).floor.toLong)}).filter(_._2 >= 1)
      val presentAndMore = present.map({ case (fingerprint, count) => (fingerprint, (count*(1-fraction)).ceil.toLong)})
      if (presentAndLess.isEmpty || (presentAndMore.isEmpty && absent.isEmpty))
        return Left("split would result in empty partition")
      val lessTotal = TotalFingerprint.fingerprintTotal(presentAndLess)
      val lessSums = TotalFingerprint.fingerprintSums(presentAndLess)
      val moreTotal = total - lessTotal
      val moreSums = for (i <- sums.indices) yield sums(i) - lessSums(i)
      (
        TotalFingerprint(names, presentAndLess, leafMetadata.updated(leafIndex, Some(columnSplits._1)), leafs, lessSums, lessTotal),
        TotalFingerprint(names, absent ++ presentAndMore, leafMetadata.updated(leafIndex, Some(columnSplits._2)), leafs, moreSums, moreTotal)
      )
    }
  }

  override def gini: Double = {
    val columnGini = leafMetadata
      .zipWithIndex
      .filter(o => o._1.isDefined && absoluteProbability(o._2)>0)
      .map(_._1.get.gini)
      .sum
    val dlGini = for (leaf <- leafs) yield {
      val key = PathKey(leaf)
      val cumulative = Seq.iterate(key, key.maxDefinitionLevel)(_.parent.get).map(absoluteProbability) :+ 1.0
      val probabilities = cumulative.head +: (0 until key.maxDefinitionLevel).map(index => cumulative(index+1) - cumulative(index))
      1- probabilities.map(p => p*p).sum
    }
    columnGini + dlGini.sum
  }

  override def equals(other: Any): Boolean = other match {
      case TotalFingerprint(otherNames, otherFingerprints, otherLeafMetadata, otherLeafs, otherSums, otherTotal) => 
        total == otherTotal && sums == otherSums && leafs == otherLeafs && leafMetadata == otherLeafMetadata && names == otherNames && fingerprints.toSet == otherFingerprints.toSet
      case _ => false
    }
}

object TotalFingerprint {
  def apply(
    names: IndexedSeq[String],
    fingerprints: Seq[(IndexedSeq[Boolean], Long)]
  ) : TotalFingerprint = {
    val leafs = names.filterNot(leaf => names.exists(node => node.startsWith(leaf) && node != leaf))
    val metadata = IndexedSeq.fill(leafs.size)(None)
    val sums = fingerprintSums(fingerprints)
    val total = fingerprintTotal(fingerprints)
    TotalFingerprint(names, fingerprints, metadata, leafs, sums, total)
  }

  def apply(
    names: IndexedSeq[String],
    fingerprints: Iterable[(IndexedSeq[Boolean], Long)],
    leafMetadata : IndexedSeq[Option[ColumnMetadata]],
    leafs: IndexedSeq[String]
  ): TotalFingerprint = TotalFingerprint(names, fingerprints.toSeq, leafMetadata, leafs, fingerprintSums(fingerprints), fingerprintTotal(fingerprints))

  def empty(names: IndexedSeq[String], leafs: IndexedSeq[String]) : TotalFingerprint = {
    val fingerprints = Seq((IndexedSeq.fill(names.size)(false), 0L))
    val sums = IndexedSeq.fill(names.length)(0L)
    val leafMetadata = IndexedSeq.fill(leafs.length)(None)
    TotalFingerprint(names, fingerprints, leafMetadata, leafs, sums, 0L)
  }

  private def fingerprintTotal(fingerprints: Iterable[(IndexedSeq[Boolean], Long)])
    = fingerprints.iterator.map(_._2).sum

  private def fingerprintSums(fingerprints: Iterable[(IndexedSeq[Boolean], Long)]) = {
    val sum = Array.fill(fingerprints.head._1.size)(0L)
    for ((fingerprint, count) <- fingerprints.iterator) {
      for {
        index <- sum.indices
      } sum(index) += count * (if (fingerprint(index)) 1 else 0)
    }
    sum
  }
}
