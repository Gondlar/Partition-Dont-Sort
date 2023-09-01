package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import scala.collection.mutable.ArrayBuffer
import TreeNode.AnyNode

class ImpossibleReplacementException(
    private val message : String,
    private val cause : Throwable = null
) extends Exception(message, cause)

/**
  * replaces a single instance of needle in the visited tree with replace
  * If needle is not found, an ImpossibleReplacementException is thrown.
  *
  * @param needle
  * @param replace
  */
final class ReplaceSubtreeVisitor[Payload](val needle: AnyNode[Payload], val replace: AnyNode[Payload])
extends SingleResultVisitor[Payload,AnyNode[Payload]] {
    private var replaced = false
    private var theResult : AnyNode[Payload] = null

    def found() = {
        replaced = true
        theResult = replace
    }

    override def visit(bucket: Bucket[Payload]) : Unit = {
        if (bucket eq needle) found()
    }

    override def visit(node: SplitByPresence[Payload]) : Unit = {
        if (node eq needle) found() else {
            node.presentKey.accept(this)
            if (replaced) {
                theResult = node.copy(presentKey = theResult)
            } else {
                node.absentKey.accept(this)
                if (replaced) theResult = node.copy(absentKey = theResult)
            }
        }
    }

    override def visit[DataType](node: SplitByValue[Payload,DataType]) : Unit = {
        if (node eq needle) found() else {
            node.less.accept(this)
            if (replaced) {
                theResult = node.copy(less = theResult)
            } else {
                node.more.accept(this)
                if (replaced) theResult = node.copy(more = theResult)
            }
        }
    }

    override def visit(spill: Spill[Payload]) : Unit = {
        if (spill eq needle) found() else {
            spill.rest.accept(this)
            if (replaced) {
                theResult match {
                    case bucket@Bucket(_) => theResult = spill.copy(rest = bucket)
                    case _ => throw new ImpossibleReplacementException("The spill partition must be replaced with a Bucket")
                }
            } else {
                spill.partitioned.accept(this)
                if (replaced) theResult = spill.copy(partitioned = theResult)
            }
        }
    }

    override def result = {
        if (!replaced) throw new ImpossibleReplacementException("Needle was not in haystack")
        assert(theResult != null)
        theResult
    }
}

trait ReplaceSubtreeOperations {
  implicit class ReplaceSubtreeNode[Payload](node: AnyNode[Payload]) {
    /**
      * Replace one ocurrence of a subtree with a different subtree.
      * The subtree is matched using object identity, i.e., you need a
      * reference into the tree.
      *
      * @param needle the subtree to be replaced
      * @param replacement the new subtree to be inserted
      */
    def replace(needle: AnyNode[Payload], replacement: AnyNode[Payload])
      = node(new ReplaceSubtreeVisitor(needle, replacement))
  }
  implicit class ReplaceSubtreeTree[Payload](tree: PartitionTree[Payload]) {
    /**
      * Replace one ocurrence of a subtree with a different subtree.
      * The subtree is matched using object identity, i.e., you need a
      * reference into the tree.
      *
      * @param needle the subtree to be replaced
      * @param replacement the new subtree to be inserted
      */
    def replace(needle: AnyNode[Payload], replacement: AnyNode[Payload])
      = tree.root = tree.root(new ReplaceSubtreeVisitor(needle, replacement))
  }
}
