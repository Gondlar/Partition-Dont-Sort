package de.unikl.cs.dbis.waves.partitions

import scala.collection.mutable.ArrayBuffer

class ImpossibleReplacementException(
    private val message : String,
    private val cause : Throwable = null
) extends Exception(message, cause)

/**
  * replaces a single instance of needle in the visited tree with replace
  *
  * @param needle
  * @param replace
  */
final class ReplaceSubtreeVisitor(val needle: TreeNode, val replace: TreeNode) extends PartitionTreeVisitor {
    private var replaced = false
    private var result : TreeNode = null

    def found() = {
        replaced = true
        result = replace
    }

    override def visit(bucket: Bucket) : Unit = {
        if (bucket eq needle) found()
    }

    override def visit(node: SplitByPresence) : Unit = {
        if (node eq needle) found() else {
            node.presentKey.accept(this)
            if (replaced) {
                result = node.copy(presentKey = result)
            } else {
                node.absentKey.accept(this)
                if (replaced) result = node.copy(absentKey = result)
            }
        }
    }

    override def visit(spill: Spill) : Unit = {
        if (spill eq needle) found() else {
            spill.rest.accept(this)
            if (replaced) {
                result match {
                    case bucket@Bucket(_) => spill.copy(rest = bucket)
                    case _ => throw new ImpossibleReplacementException("The spill partition must be replaced with a Bucket")
                }
            } else {
                spill.partitioned.accept(this)
                if (replaced) result = spill.copy(partitioned = result)
            }
        }
    }

    def getResult = {
        if (!replaced) throw new ImpossibleReplacementException("Needle was not in haystack")
        assert(result != null)
        result
    }
}