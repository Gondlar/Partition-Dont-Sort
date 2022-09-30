package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.util.operators.{Grouper,NullGrouper}

import de.unikl.cs.dbis.waves.partitions.TreeNode
import de.unikl.cs.dbis.waves.partitions.PartitionTreePath
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import de.unikl.cs.dbis.waves.partitions.visitors.CollectBucketMetadataVisitor
import de.unikl.cs.dbis.waves.partitions.visitors.CollectBucketsVisitor
import org.apache.hadoop.fs.Path
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.partitions.visitors.MapVisitor
import de.unikl.cs.dbis.waves.partitions.visitors.ReplaceSubtreeVisitor

import TreeNode.AnyNode
import de.unikl.cs.dbis.waves.partitions.PartitionTree
import org.apache.spark.sql.types.StructType

/**
  * Implements a splitter which (re-)partitions the data according to the given
  * shape. It can replace (parts of) existing trees in-place
  */
class PredefinedSplitter(
  shape: AnyNode[String],
  subtreePath: Seq[PartitionTreePath] = Seq.empty,
) extends GroupedSplitter {

  /**
    * We maintain that tree is:
    * - null when the splitter is not prepared
    * - Left when the filesystem contains an existing PartitionTree
    * - Right when the filesystem contains no tree but we can create a new one
    *   from the given shape
    */
  private var tree: Either[PartitionTree[String],StructType] = null
  
  override protected def splitGrouper: Grouper = NullGrouper

  override def prepare(df: DataFrame, path: String): GroupedSplitter = {
    super.prepare(df, path)
    tree = getHDFS.read() match {
      case Some(value) => Left(value)
      case None if subtreePath.isEmpty => Right(df.schema)
      case _ => throw new IllegalArgumentException(
        "the target directory may only be empty when the subtree path points to the root"
      )
    }
    this
  }
  
  override protected def split(df: DataFrame): Seq[DataFrame] = {
    val metadata = shape(new CollectBucketMetadataVisitor[String]())
    if (metadata.length > 1) {
      metadata.map(metadata => df.filter(makeFilter(df, metadata)))
    } else Seq(df)
  }

  private def makeFilter(df: DataFrame, metadata: PartitionMetadata) = {
    val absent = metadata.getAbsent.map(k => df.col(k.toSpark).isNull)
    val present = metadata.getPresent.map(k => df.col(k.toSpark).isNotNull)
    (absent ++ present).reduce((lhs, rhs) => lhs && rhs)
  }

  override protected def buildTree(buckets: Seq[PartitionFolder]): PartitionTree[String] = {
    val newSubtree = shape(new MapVisitor[String,String]((oldPath, index) => buckets(index).name))
    tree match {
      case Left(existingTree) => {
        existingTree.replace(subtreePath, newSubtree)
        existingTree
      }
      case Right(schema) => new PartitionTree(schema, newSubtree)
    }
  }
}
