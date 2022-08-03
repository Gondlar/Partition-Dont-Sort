package de.unikl.cs.dbis.waves.partitions

import org.apache.spark.sql.sources.{
    Filter,
    EqualTo,
    EqualNullSafe,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    In,
    IsNull,
    IsNotNull,
    And,
    Or,
    Not,
    StringStartsWith,
    StringEndsWith,
    StringContains,
    AlwaysTrue,
    AlwaysFalse
}
import scala.collection.mutable.ArrayBuffer
import de.unikl.cs.dbis.waves.util.{PathKey,Ternary,TernarySet}

final class CollectFilteredBucketsVisitor(val filters: Iterable[Filter]) extends PartitionTreeVisitor {
    private val buckets = ArrayBuffer.empty[Bucket]

    override def visit(bucket: Bucket) : Unit = buckets += bucket

    override def visit(node: SplitByPresence) : Unit = {
        if (CollectFilteredBucketsVisitor.eval(node.key, true, filters).isFulfillable)
            node.presentKey.accept(this)
        if (CollectFilteredBucketsVisitor.eval(node.key, false, filters).isFulfillable)
            node.absentKey.accept(this)
    }

    override def visit(spill: Spill) : Unit = {
        buckets += spill.rest
        spill.partitioned.accept(this)
    }

    def iter = buckets.iterator
}

object CollectFilteredBucketsVisitor {
    private def operationOnKey(partitionKey: PathKey, present : Boolean, filterKey: String) = {
        // we're ignoring the possibly null value here
        if (present) TernarySet.any
        else if (partitionKey.contains(PathKey(filterKey))) TernarySet.alwaysUnknown
        else TernarySet.any
    }

    def eval(key : PathKey, present : Boolean, filters: Iterable[Filter]) : TernarySet
        = filters.map(filter => eval(key, present,filter)).fold(TernarySet.any)((lhs, rhs) => lhs && rhs)

    def eval(key : PathKey, present : Boolean, filter: Filter) : TernarySet = filter match {
        case AlwaysTrue() => TernarySet.alwaysTrue
        case AlwaysFalse() => TernarySet.alwaysFalse
        case And(left, right) => eval(key, present, left) && eval(key, present, right)
        case Or(left, right) => eval(key, present, left) || eval(key, present, right)
        case Not(child) => !eval(key, present, child)
        case IsNull(attribute) => {
            if (present) {
                if (key == PathKey(attribute)) TernarySet.alwaysFalse
                else TernarySet.any
            } else {
                if (key.contains(PathKey(attribute))) TernarySet.alwaysTrue
                else TernarySet.any
            }
        }
        case IsNotNull(attribute) => {
            if (present) {
                if (key == PathKey(attribute)) TernarySet.alwaysTrue
                else TernarySet.any
            } else {
                if (key.contains(PathKey(attribute))) TernarySet.alwaysFalse
                else TernarySet.any
            }
        }
        case EqualTo(attribute, value) => operationOnKey(key, present, attribute)
        case GreaterThan(attribute, value) => operationOnKey(key, present, attribute)
        case GreaterThanOrEqual(attribute, value) => operationOnKey(key, present, attribute)
        case LessThan(attribute, value) => operationOnKey(key, present, attribute)
        case LessThanOrEqual(attribute, value) => operationOnKey(key, present, attribute)
        case In(attribute, values) => operationOnKey(key, present, attribute)
        case StringStartsWith(attribute, value) => operationOnKey(key, present, attribute)
        case StringEndsWith(attribute, value) => operationOnKey(key, present, attribute)
        case StringContains(attribute, value) => operationOnKey(key, present, attribute)
        case EqualNullSafe(attribute, value) => ??? //TODO
    }
}
