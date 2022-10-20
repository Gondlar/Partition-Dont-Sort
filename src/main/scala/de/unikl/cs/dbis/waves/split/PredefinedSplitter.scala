package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import de.unikl.cs.dbis.waves.partitions.{TreeNode,PartitionTree,PartitionMetadata}
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.util.operators.{Grouper,NullGrouper}
import de.unikl.cs.dbis.waves.util.PartitionFolder

import TreeNode.AnyNode

/**
  * Implements a splitter which (re-)partitions the data according to the given
  * shape. It can replace (parts of) existing trees in-place
  */
class PredefinedSplitter(
  shape: AnyNode[String],
  subtreeMetadata: PartitionMetadata = PartitionMetadata()
) extends GroupedSplitter {

  /**
    * We maintain that tree is:
    * - null when the splitter is not prepared
    * - Left when the filesystem contains an existing PartitionTree
    * - Right when the filesystem contains no tree but we can create a new one
    *   from the given shape
    */
  private var tree: Either[PartitionTree[String],StructType] = null

  private var positions: Map[Int,Int] = Map.empty
  
  override protected def splitGrouper: Grouper = NullGrouper

  override def prepare(df: DataFrame, path: String): GroupedSplitter = {
    super.prepare(df, path)
    tree = getHDFS.read() match {
      case Some(value) => Left(value)
      case None if subtreeMetadata.isRoot => Right(df.schema)
      case _ => throw new IllegalArgumentException(
        "the target directory may only be empty when the subtree path points to the root"
      )
    }
    this
  }
  
  override protected def split(df: DataFrame): (Seq[DataFrame], Seq[PartitionMetadata]) = {
    val metadata = shape.metadata(subtreeMetadata)
    positions = bucketPositions(metadata)
    val nonSpill = metadata.filter(!_.isSpillBucket)
    if (nonSpill.length > 1) {
      (nonSpill.map(metadata => df.filter(makeFilter(df, metadata))), nonSpill)
    } else (Seq(df), nonSpill)
  }

  private def bucketPositions(metadata: Seq[PartitionMetadata])
    = metadata.zipWithIndex
              .filter(!_._1.isSpillBucket)
              .map(_._2)
              .zipWithIndex
              .toMap

  private def makeFilter(df: DataFrame, metadata: PartitionMetadata) = {
    val absent = metadata.getAbsent.map(k => df.col(k.toSpark).isNull)
    val present = metadata.getPresent.map(k => df.col(k.toSpark).isNotNull)
    (absent ++ present).reduce((lhs, rhs) => lhs && rhs)
  }

  override protected def buildTree(buckets: Seq[PartitionFolder]): PartitionTree[String] = {
    val newSubtree = treeByShape(buckets)
    tree match {
      case Left(existingTree) => {
        existingTree.replace(subtreeMetadata.getPath, newSubtree)
        existingTree
      }
      case Right(schema) => new PartitionTree(schema, sorter, newSubtree)
    }
  }

  private def treeByShape(buckets: Seq[PartitionFolder]) = {
    implicit val fs = getHDFS.fs
    shape.map((oldPath, index) => {
      positions.get(index) match {
        case None => {
          new PartitionFolder(getPath, oldPath, false).mkdir
          oldPath
        }
        case Some(value) => buckets(value).name
      }
    })
  }
}
