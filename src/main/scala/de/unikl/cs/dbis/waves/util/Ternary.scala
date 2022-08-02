package de.unikl.cs.dbis.waves.util

/**
  * A Ternary represents a value in ternary logic.
  * 
  * Possible Values are True, False, and Unknown
  */
sealed trait Ternary {
    def &&(other : Ternary) : Ternary = this match {
        case True => other
        case Unknown if other == False => False
        case Unknown => Unknown
        case False => False
    }
    def ||(other : Ternary) : Ternary = this match {
        case True => True
        case Unknown if other == True => True
        case Unknown => Unknown
        case False => other
    }
    def unary_! : Ternary = this match {
        case True => False
        case False => True
        case Unknown => Unknown
    }
}
final object True extends Ternary
final object False extends Ternary
final object Unknown extends Ternary

/**
  * A TernarySet represents a set of Ternary values which are manipulated together 
  *
  * @param containsTrue whether the set contains true values
  * @param containsUnknown whether the set contains unknown values
  * @param containsFalse whether the set contains false values
  */
final case class TernarySet(
    containsTrue : Boolean,
    containsUnknown : Boolean,
    containsFalse : Boolean,
) {
    /**
      * Compute the list of distinct Ternary values that can be contained in this set 
      *
      * @return the list
      */
    def toSeq : Seq[Ternary] = {
        val trueList = if (containsTrue) Seq[Ternary](True) else Seq.empty[Ternary]
        val unknownList = if (containsUnknown) Seq[Ternary](Unknown) else Seq.empty[Ternary]
        val falseList = if (containsFalse) Seq[Ternary](False) else Seq.empty[Ternary]
        trueList ++ unknownList ++ falseList
    }

    private def lift1(op : Ternary => Ternary) : TernarySet = {
        val combinations = for { v <- toSeq } yield op(v)
        TernarySet(combinations.contains(True), combinations.contains(Unknown), combinations.contains(False))
    }
    private def lift2(op : (Ternary, Ternary) => Ternary, other : TernarySet) : TernarySet = {
        val combinations = for { left <- toSeq; right <- other.toSeq } yield op(left, right)
        TernarySet(combinations.contains(True), combinations.contains(Unknown), combinations.contains(False))
    }

    def &&(other: TernarySet) : TernarySet = lift2((lhs, rhs) => lhs && rhs, other)
    def ||(other: TernarySet) : TernarySet = lift2((lhs, rhs) => lhs || rhs, other)
    def unary_! : TernarySet = lift1(v => !v)

    /**
      * A set is fulfillable when it contains the (ternary) value True
      *
      * @return true iff the set contains True, otherwise false
      */
    def isFulfillable = containsTrue

    /**
      * A set is not unfulfillable if it contains a value other than (ternary) False
      *
      * @return true iff the set contains True or Unknown, otherwise false
      */
    def isNotUnfulfillable = containsTrue || containsUnknown
}

object TernarySet {
    /**
      * A TernarySet containing all Ternary values
      */
    val any = TernarySet(true, true, true)

    /**
      * An empty TernarySet 
      */
    val empty = TernarySet(false, false, false)

    /**
      * A TernarySet containing only the value True
      */
    val alwaysTrue = TernarySet(true, false, false)

    /**
      * A TernarySet containing only the value Unknown
      */
    val alwaysUnknown = TernarySet(false, true, false)

    /**
      * A TernarySet containing only the value False
      */
    val alwaysFalse = TernarySet(false, false, true)
}