package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

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

import TreeNode.AnyNode

/**
  * Find all Buckets in a PartitionTree that can contain entries
  * satisfying a list of predicates.
  * If no predicates are given, it behvaes like a [[CollectBucketsVisitor]]
  *
  * @param filters the predicates
  */
final class CollectFilteredBucketsVisitor[Payload](
  val filters: Iterable[Filter]
) extends SingleResultVisitor[Payload,Seq[Bucket[Payload]]] {
    private val buckets = Seq.newBuilder[Bucket[Payload]]

    override def visit(bucket: Bucket[Payload]) : Unit = buckets += bucket

    override def visit(node: SplitByPresence[Payload]) : Unit = {
        if (CollectFilteredBucketsVisitor.eval(node.key, true, filters).isFulfillable)
            node.presentKey.accept(this)
        if (CollectFilteredBucketsVisitor.eval(node.key, false, filters).isFulfillable)
            node.absentKey.accept(this)
    }

    override def visit(node: SplitByValue[Payload]) : Unit = {
        //TODO check if predicates are filfillable given the value split
        if (CollectFilteredBucketsVisitor.eval(node.key, true, filters).isFulfillable)
            node.less.accept(this)
        node.more.accept(this)
    }

    override def visit(spill: Spill[Payload]) : Unit = {
        buckets += spill.rest
        spill.partitioned.accept(this)
    }

    override def visit(nway: EvenNWay[Payload]): Unit = {
      for (child <- nway.children)
        child.accept(this)
    }

    override def result = buckets.result
}

object CollectFilteredBucketsVisitor {
    private def operationOnKey(partitionKey: PathKey, present : Boolean, filterKey: String) = {
        // we're ignoring the possibly null value here
        if (present) TernarySet.any
        else if (partitionKey isPrefixOf PathKey(filterKey)) TernarySet.alwaysUnknown
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
                if (key isPrefixOf PathKey(attribute)) TernarySet.alwaysTrue
                else TernarySet.any
            }
        }
        case IsNotNull(attribute) => {
            if (present) {
                if (key == PathKey(attribute)) TernarySet.alwaysTrue
                else TernarySet.any
            } else {
                if (key isPrefixOf PathKey(attribute)) TernarySet.alwaysFalse
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

trait CollectFilteredBucketOperations {
  implicit class CollectFilteredBucketsNode[Payload](node: AnyNode[Payload]) {
    /**
      * Find all Buckets with contents which can satisfy the given filters
      * If no filters are given, this is equivalent to [[getBuckets]]
      *
      * @param filters A collection of filters
      * @return the Buckets
      */
    def bucketsWith(filters: Iterable[Filter])
      = node(new CollectFilteredBucketsVisitor[Payload](filters))
  }
  implicit class CollectFilteredBucketsTree[Payload](tree: PartitionTree[Payload]) {
    /**
      * Find all Buckets with contents which can satisfy the given filters
      * If no filters are given, this is equivalent to [[getBuckets]]
      *
      * @param filters A collection of filters
      * @return the Buckets
      */
    def bucketsWith(filters: Iterable[Filter])
      = tree.root(new CollectFilteredBucketsVisitor[Payload](filters))
  }
}
