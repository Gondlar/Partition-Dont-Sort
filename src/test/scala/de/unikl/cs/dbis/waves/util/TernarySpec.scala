package de.unikl.cs.dbis.waves.util

import de.unikl.cs.dbis.waves.WavesSpec

class TernarySpec extends WavesSpec {

    "A Ternary" when {
        "True" should {
            "negate to false" in {
                val t : Ternary = True
                !t should equal (False)
            }
            "and to the other value" in {
                val t : Ternary = True
                Given("true")
                t && True should equal (True)
                Given("unknown")
                t && Unknown should equal (Unknown)
                Given("false")
                t && False should equal (False)
            }
            "or to true" in {
                val t : Ternary = True
                Given("true")
                t || True should equal (True)
                Given("unknown")
                t || Unknown should equal (True)
                Given("false")
                t || False should equal (True)
            }
        }
        "False" should {
            "negate to true" in {
                val t : Ternary = False
                !t should equal (True)
            }
            "and to false" in {
                val t : Ternary = False
                Given("true")
                t && True should equal (False)
                Given("unknown")
                t && Unknown should equal (False)
                Given("false")
                t && False should equal (False)
            }
            "or to the other value" in {
                val t : Ternary = False
                Given("true")
                t || True should equal (True)
                Given("unknown")
                t || Unknown should equal (Unknown)
                Given("false")
                t || False should equal (False)
            }
        }
        "Unknown" should {
            "negate to unknown" in {
                val t : Ternary = Unknown
                !t should equal (Unknown)
            }
            "and to unknown or false" in {
                val t : Ternary = Unknown
                Given("false")
                t && False should equal (False)
                Given("true")
                t && True should equal (Unknown)
                Given("unknown")
                t && Unknown should equal (Unknown)
            }
            "or to unknown or true" in {
                val t : Ternary = Unknown
                Given("true")
                t || True should equal (True)
                Given("unknown")
                t || Unknown should equal (Unknown)
                Given("false")
                t || False should equal (Unknown)
            }
        }
    }
    "A TernarySet" when {
        "it is empty" should {
            "represent an empty list" in {
                TernarySet.empty.toSeq should equal (Seq.empty[Ternary])
            }
            "be unchanged by all logical operators" in {
                When("ANDing any")
                TernarySet.empty && TernarySet.any should equal (TernarySet.empty)
                When("ANDing true")
                TernarySet.empty && TernarySet.alwaysTrue should equal (TernarySet.empty)
                When("ORing any")
                TernarySet.empty || TernarySet.any should equal (TernarySet.empty)
                When("ORing unknown")
                TernarySet.empty || TernarySet.alwaysUnknown should equal (TernarySet.empty)
                When("Negating")
                !TernarySet.empty should equal (TernarySet.empty)
            }
            "never be true" in {
                TernarySet.empty should not be 'Fulfillable
            }
            "never be non-false" in {
                TernarySet.empty should not be 'NotUnfulfillable
            }
        }
        "it contains values" should {
            "represent them correctly as a list" in {
                When("Any")
                TernarySet.any.toSeq should contain theSameElementsAs Seq(True, Unknown, False) 
                When("False")
                TernarySet.alwaysFalse.toSeq should contain theSameElementsAs Seq(False) 
                When("True")
                TernarySet.alwaysTrue.toSeq should contain theSameElementsAs Seq(True) 
                When("Unknown")
                TernarySet.alwaysUnknown.toSeq should contain theSameElementsAs Seq(Unknown) 
                When("Combination")
                TernarySet(true, true, false).toSeq should contain theSameElementsAs Seq(True, Unknown) 
            }
            "be true if it contains true" in {
                When("Any")
                TernarySet.any shouldBe 'Fulfillable
                When("False")
                TernarySet.alwaysFalse should not be 'Fulfillable
                When("True")
                TernarySet.alwaysTrue shouldBe 'Fulfillable
                When("Unknown")
                TernarySet.alwaysUnknown should not be 'Fulfillable
                When("Combination")
                TernarySet(true, true, false) shouldBe 'Fulfillable
            }
            "be non-false if it contains true or unknown" in {
                When("Any")
                TernarySet.any shouldBe 'NotUnfulfillable
                When("False")
                TernarySet.alwaysFalse should not be 'NotUnfulfillable
                When("True")
                TernarySet.alwaysTrue shouldBe 'NotUnfulfillable
                When("Unknown")
                TernarySet.alwaysUnknown shouldBe 'NotUnfulfillable
                When("Combination")
                TernarySet(true, true, false) shouldBe 'NotUnfulfillable
            }
            "negate correctly" in {
                When("True")
                !TernarySet.alwaysTrue should equal (TernarySet.alwaysFalse)
                When("FAlse")
                !TernarySet.alwaysFalse should equal (TernarySet.alwaysTrue)
                When("Unknown")
                !TernarySet.alwaysUnknown should equal (TernarySet.alwaysUnknown)
                When("Any")
                !TernarySet.any should equal (TernarySet.any)
                When("Combination")
                !TernarySet(true, true, false) should equal (TernarySet(false, true, true))
            }
            "and correctly" in {
                Given("true and true")
                TernarySet.alwaysTrue && TernarySet.alwaysTrue should equal (TernarySet.alwaysTrue)
                Given("true and unknown")
                TernarySet.alwaysTrue && TernarySet.alwaysUnknown should equal (TernarySet.alwaysUnknown)
                Given("true and false")
                TernarySet.alwaysTrue && TernarySet.alwaysFalse should equal (TernarySet.alwaysFalse)
                Given("unknown and true")
                TernarySet.alwaysUnknown && TernarySet.alwaysTrue should equal (TernarySet.alwaysUnknown)
                Given("unknown and unknown")
                TernarySet.alwaysUnknown && TernarySet.alwaysUnknown should equal (TernarySet.alwaysUnknown)
                Given("unknown and false")
                TernarySet.alwaysUnknown && TernarySet.alwaysFalse should equal (TernarySet.alwaysFalse)
                Given("false and true")
                TernarySet.alwaysFalse && TernarySet.alwaysTrue should equal (TernarySet.alwaysFalse)
                Given("false and unknown")
                TernarySet.alwaysFalse && TernarySet.alwaysUnknown should equal (TernarySet.alwaysFalse)
                Given("false and false")
                TernarySet.alwaysFalse && TernarySet.alwaysFalse should equal (TernarySet.alwaysFalse)
                Given("a free combination")
                TernarySet(true, true, false) && TernarySet(false, true, true) should equal (TernarySet(false, true, true))
            }
            "or correctly" in {
                Given("true and true")
                TernarySet.alwaysTrue || TernarySet.alwaysTrue should equal (TernarySet.alwaysTrue)
                Given("true and unknown")
                TernarySet.alwaysTrue || TernarySet.alwaysUnknown should equal (TernarySet.alwaysTrue)
                Given("true and false")
                TernarySet.alwaysTrue || TernarySet.alwaysFalse should equal (TernarySet.alwaysTrue)
                Given("unknown and true")
                TernarySet.alwaysUnknown || TernarySet.alwaysTrue should equal (TernarySet.alwaysTrue)
                Given("unknown and unknown")
                TernarySet.alwaysUnknown || TernarySet.alwaysUnknown should equal (TernarySet.alwaysUnknown)
                Given("unknown and false")
                TernarySet.alwaysUnknown || TernarySet.alwaysFalse should equal (TernarySet.alwaysUnknown)
                Given("false and true")
                TernarySet.alwaysFalse || TernarySet.alwaysTrue should equal (TernarySet.alwaysTrue)
                Given("false and unknown")
                TernarySet.alwaysFalse || TernarySet.alwaysUnknown should equal (TernarySet.alwaysUnknown)
                Given("false and false")
                TernarySet.alwaysFalse || TernarySet.alwaysFalse should equal (TernarySet.alwaysFalse)
                Given("a free combination")
                TernarySet(true, true, false) || TernarySet(false, true, true) should equal (TernarySet(true, true, false))
            }
        }
    }
    
}