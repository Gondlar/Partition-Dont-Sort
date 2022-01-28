package de.unikl.cs.dbis.waves.util

sealed trait Ternary {
    def &&(other : Ternary) : Ternary = this match {
        case True() => other
        case Unknown() if other == False() => False()
        case Unknown() => Unknown()
        case False() => False()
    }
    def ||(other : Ternary) : Ternary = this match {
        case True() => True()
        case Unknown() if other == True() => True()
        case Unknown() => Unknown()
        case False() => other
    }
    def unary_! : Ternary = this match {
        case True() => False()
        case False() => True()
        case Unknown() => Unknown()
    }
}
final case class True() extends Ternary
final case class False() extends Ternary
final case class Unknown() extends Ternary

final case class TernarySet(
    containsTrue : Boolean,
    containsUnknown : Boolean,
    containsFalse : Boolean,
) {
    def toSeq : Seq[Ternary] = {
        val trueList = if (containsTrue) Seq[Ternary](True()) else Seq.empty[Ternary]
        val unknownList = if (containsUnknown) Seq[Ternary](Unknown()) else Seq.empty[Ternary]
        val falseList = if (containsFalse) Seq[Ternary](False()) else Seq.empty[Ternary]
        trueList ++ unknownList ++ falseList
    }

    private def lift1(op : Ternary => Ternary) : TernarySet = {
        val combinations = for { v <- toSeq } yield op(v)
        TernarySet(combinations.contains(True()), combinations.contains(Unknown()), combinations.contains(False()))
    }
    private def lift2(op : (Ternary, Ternary) => Ternary, other : TernarySet) : TernarySet = {
        val combinations = for { left <- toSeq; right <- other.toSeq } yield op(left, right)
        TernarySet(combinations.contains(True()), combinations.contains(Unknown()), combinations.contains(False()))
    }

    def &&(other: TernarySet) : TernarySet = lift2((lhs, rhs) => lhs && rhs, other)
    def ||(other: TernarySet) : TernarySet = lift2((lhs, rhs) => lhs || rhs, other)
    def unary_! : TernarySet = lift1(v => !v)

    def canBeTrue = containsTrue
    def canBeNotFalse = containsTrue || containsUnknown
}

object TernarySet {
    def any = TernarySet(true, true, true)
    def alwaysTrue = TernarySet(true, false, false)
    def alwaysUnknown = TernarySet(false, true, false)
    def alwaysFalse = TernarySet(false, false, true)
}