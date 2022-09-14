package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture

import TreeNode.AnyNode

class NavigatePathVisitorSpec extends WavesSpec
  with PartitionTreeFixture {

  "A NavigatePathVisitor" when {
    "traversing a tree" should {
      "call the callbacks in the right order" in {
        val visitor = TestNavigatePathVisitor(Seq(Partitioned, Present))
        spill.accept(visitor)
        visitor.error shouldBe (false)
        visitor.down.result() should equal (Seq(
          (spill, split, Partitioned),
          (split, split.presentKey, Present)
        ))
        visitor.up.result() should equal (Seq(
          (split.presentKey, split, Present),
          (split, spill, Partitioned)
        ))
        visitor.last should equal (Some(split.presentKey))
      }
      "call the callback corretly on for an invalid path" in {
        val visitor = TestNavigatePathVisitor(Seq(Partitioned, Present, Absent), false)
        spill.accept(visitor)
        visitor.error shouldBe (true)
        visitor.down.result() should equal (Seq(
          (spill, split, Partitioned),
          (split, split.presentKey, Present)
        ))
        visitor.up.result() should equal (Seq(
          (split.presentKey, split, Present),
          (split, spill, Partitioned)
        ))
        visitor.last should contain (split.presentKey)
        visitor.lastStep should contain (Absent)
      }
    }
    "visiting a Bucket" should {
      "find that bucket for an empty path" in {
        val visitor = TestNavigatePathVisitor(Seq.empty)
        bucket.accept(visitor)
        visitor.last should contain (bucket)
      }
      "produce an error for non-empty paths" in {
        Given("a path and a visitor")
        val testpath = Seq(Present)
        val visitor = TestNavigatePathVisitor(testpath)

        Then("visiting throws an exception")
        val ex = the [InvalidPathException] thrownBy (bucket.accept(visitor))

        And("The correct callbacks were called")
        visitor.last should contain (bucket)
        visitor.error shouldBe (true)
        visitor.lastStep should contain (Present)

        And("the exception contains the correct data")
        ex.path should equal (testpath)
        ex.step should equal (Present)
      }
    }
    "visiting a Split" should {
      "be able to navigate to the present side" in {
        val visitor = TestNavigatePathVisitor(Seq(Present))
        split.accept(visitor)
        visitor.last should contain (split.presentKey)
      }
      "be able to navigate to the absent side" in {
        val visitor = TestNavigatePathVisitor(Seq(Absent))
        split.accept(visitor)
        visitor.last should contain (split.absentKey)
      }
      "produce an error for other paths" in {
        Given("a path and a visitor")
        val testpath = Seq(Rest)
        val visitor = TestNavigatePathVisitor(testpath)

        Then("visiting throws an exception")
        val ex = the [InvalidPathException] thrownBy (split.accept(visitor))

        And("The correct callbacks were called")
        visitor.last should contain (split)
        visitor.error shouldBe (true)
        visitor.lastStep should contain (Rest)

        And("the exception contains the correct data")
        ex.path should equal (testpath)
        ex.step should equal (Rest)
      }
      "return the root for empty paths" in {
        val visitor = TestNavigatePathVisitor(Seq.empty)
        split.accept(visitor)
        visitor.last should contain (split)
      }
    }
    "visiting a Spill" should {
      "be able to navigate to the partitioned side" in {
        val visitor = TestNavigatePathVisitor(Seq(Partitioned))
        spill.accept(visitor)
        visitor.last should contain (spill.partitioned)
      }
      "be able to navigate to the rest side" in {
        val visitor = TestNavigatePathVisitor(Seq(Rest))
        spill.accept(visitor)
        visitor.last should contain (spill.rest)
      }
      "produce an error for other paths" in {
        Given("a path and a visitor")
        val testpath = Seq(Present)
        val visitor = TestNavigatePathVisitor(testpath)

        Then("visiting throws an exception")
        val ex = the [InvalidPathException] thrownBy (spill.accept(visitor))

        And("The correct callbacks were called")
        visitor.last should contain (spill)
        visitor.error shouldBe (true)
        visitor.lastStep should contain (Present)

        And("the exception contains the correct data")
        ex.path should equal (testpath)
        ex.step should equal (Present)
      }
      "return the root for empty paths" in {
        val visitor = TestNavigatePathVisitor(Seq.empty)
        spill.accept(visitor)
        visitor.last should contain (spill)
      }
    }
  }
  
  case class TestNavigatePathVisitor(
    path: Iterable[PartitionTreePath],
    doError: Boolean = true
  ) extends NavigatePathVisitor[String](path) {
    val down = Seq.newBuilder[(AnyNode[String],AnyNode[String], PartitionTreePath)]
    val up = Seq.newBuilder[(AnyNode[String],AnyNode[String], PartitionTreePath)]
    var last : Option[AnyNode[String]] = None
    var lastStep : Option[PartitionTreePath] = None
    private var returning = false
    var error = false

    override protected def navigateDown[Step <: PartitionTreePath, From <: TreeNode[String,Step]](
      from: From, to: AnyNode[String], step: Step
    ): Unit = {
      returning shouldBe (false)
      error shouldBe (false)
      down += ((from, to, step))
    }

    override protected def navigateUp[Step <: PartitionTreePath, To <: TreeNode[String,Step]](
      from: AnyNode[String], to: To, step: Step
    ): Unit = {
      returning shouldBe (true)
      if (doError) {
        error shouldBe (false)
      }
      up += ((from, to, step))
    }

    override protected def endOfPath(node: AnyNode[String]): Unit = {
      returning shouldBe (false)
      error shouldBe (false)
      last = Some(node)
      returning = true
    }

    override def invalidStep(node: AnyNode[String], step: PartitionTreePath): Unit = {
      returning shouldBe (false)
      error shouldBe (false)
      last = Some(node)
      lastStep = Some(step)
      returning = true
      error = true
      if (doError) super.invalidStep(node, step)
    }
  }
}