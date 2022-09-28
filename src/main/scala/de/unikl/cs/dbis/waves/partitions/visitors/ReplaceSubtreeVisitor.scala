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
extends PartitionTreeVisitor[Payload] {
    private var replaced = false
    private var result : AnyNode[Payload] = null

    def found() = {
        replaced = true
        result = replace
    }

    override def visit(bucket: Bucket[Payload]) : Unit = {
        if (bucket eq needle) found()
    }

    override def visit(node: SplitByPresence[Payload]) : Unit = {
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

    override def visit(spill: Spill[Payload]) : Unit = {
        if (spill eq needle) found() else {
            spill.rest.accept(this)
            if (replaced) {
                result match {
                    case bucket@Bucket(_) => result = spill.copy(rest = bucket)
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