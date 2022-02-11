package de.unikl.cs.dbis.waves.partitions

import org.apache.spark.sql.catalyst.expressions.{Bucket =>_, _}
import org.apache.spark.sql.types.BooleanType

import scala.collection.mutable.ArrayBuffer
import de.unikl.cs.dbis.waves.util.{PathKey,Ternary,TernarySet}

final class CollectFilteredBucketsVisitor(val filters: Iterable[Expression]) extends PartitionTreeVisitor {
    private val buckets = ArrayBuffer.empty[Bucket]

    override def visit(bucket: Bucket) : Unit = buckets.addOne(bucket)

    override def visit(node: PartitionByInnerNode) : Unit = {
        if (CollectFilteredBucketsVisitor.eval(node.key, true, filters).canBeTrue)
            node.presentKey.accept(this)
        if (CollectFilteredBucketsVisitor.eval(node.key, false, filters).canBeTrue)
            node.absentKey.accept(this)
    }

    override def visit(spill: Spill) : Unit = {
        buckets.addOne(spill.rest)
        spill.partitioned.accept(this)
    }

    def iter = buckets.iterator
}

object CollectFilteredBucketsVisitor {
    def eval(key : PathKey, present : Boolean, filters: Iterable[Expression]) : TernarySet
        = filters.map(filter => eval(key, present,filter)).fold(TernarySet.any)(_&&_)

    def eval(key : PathKey, present : Boolean, filter : Expression) : TernarySet = filter match {
        case And(left, right) => eval(key, present, left) && eval(key, present, right)
        case Or(left, right) => eval(key, present, left) || eval(key, present, right)
        case Not(expr) => !eval(key, present, expr)
        case Literal(true, BooleanType) => TernarySet.alwaysTrue
        case Literal(false, BooleanType) => TernarySet.alwaysFalse
        case IsNull(child) if isRecursivelyNullIntolerant(child) => { //TODO probably need to check deterministic and nullable as well
            val references = child.references
            if (present) {
                if (references.forall(attr => key == PathKey(attr.name)))
                    TernarySet.alwaysFalse else TernarySet.any
            } else {
                if (references.exists(attr => key.contains(PathKey(attr.name))))
                    TernarySet.alwaysTrue else TernarySet.any
            }
        }
        case IsNotNull(child) if isRecursivelyNullIntolerant(child) => {  //TODO probably need to check deterministic and nullable as well
            val references = child.references
            if (present) {
                if (references.forall(attr => key == PathKey(attr.name)))
                    TernarySet.alwaysTrue else TernarySet.any
            } else {
                if (references.exists(attr => key.contains(PathKey(attr.name))))
                    TernarySet.alwaysFalse else TernarySet.any
            }
        }
        case _ if !present && isRecursivelyNullIntolerant(filter)
                           && filter.references.exists(attr => key.contains(PathKey(attr.name)))
            => TernarySet.alwaysUnknown
        case _ => TernarySet.any
    }

    def isRecursivelyNullIntolerant(expression : Expression) : Boolean = expression match {
        case _: NullIntolerant => expression
                                    .children
                                    .forall(child => isRecursivelyNullIntolerant(child))
        case _ => false
    }
}
