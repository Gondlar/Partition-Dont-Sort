package de.unikl.cs.dbis.waves

import de.unikl.cs.dbis.waves.partitions.TreeNode

import TreeNode.AnyNode
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.MatchResult
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.Spill
import de.unikl.cs.dbis.waves.partitions.SplitByPresence
import de.unikl.cs.dbis.waves.partitions.PartitionTree
import de.unikl.cs.dbis.waves.partitions.SplitByValue

trait PartitionTreeMatchers {
  class StructureMatcher[Payload](right: AnyNode[Payload]) extends Matcher[AnyNode[Payload]] {

    override def apply(left: AnyNode[Payload]): MatchResult
      = MatchResult(matches(left, right),
                    s"""$left does not have the same structure as $right""",
                    s"""$left has the same structure as $right""")

    def matches(lhs: AnyNode[Payload], rhs: AnyNode[Payload]): Boolean = lhs match {
      case Bucket(_) => rhs.isInstanceOf[Bucket[Payload]]
      case Spill(lhsPartitioned, _) => rhs match {
        case Spill(rhsPartitioned, _) => matches(lhsPartitioned,rhsPartitioned)
        case _ => false
      }
      case SplitByPresence(lhsKey, lhsPresent, lhsAbsent) => rhs match {
        case SplitByPresence(rhsKey, rhsPresent, rhsAbsent)
          => lhsKey == rhsKey && matches(lhsPresent, rhsPresent) && matches(lhsAbsent, rhsAbsent)
        case _ => false 
      }
      case SplitByValue(lhsPivot, lhsKey, lhsLess, lhsMore) => rhs match {
        case SplitByValue(rhsPivot, rhsKey, rhsLess, rhsMore)
          => lhsPivot == rhsPivot && lhsKey == rhsKey && matches(lhsLess, rhsLess) && matches(lhsMore, rhsMore)
        case _ => false
      }
    }
  }

  class TreeMatcher[Payload](right: PartitionTree[Payload]) extends Matcher[PartitionTree[Payload]] {

    override def apply(left: PartitionTree[Payload]): MatchResult = {
      if (left.globalSchema != right.globalSchema) {
        MatchResult(false, s"""$left has a different schema than $right""", "should never show up")
      } else {
        haveTheSameStructureAs(right.root)(left.root)
      }
    }

  }

  def haveTheSameStructureAs[Payload](expected: AnyNode[Payload])
    = new StructureMatcher(expected)

  def haveTheSameStructureAs[Payload](expected: PartitionTree[Payload])
    = new TreeMatcher(expected)
}
