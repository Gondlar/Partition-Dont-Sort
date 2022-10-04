package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec
import org.scalatest.Inspectors._

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterEach

import de.unikl.cs.dbis.waves.util.PathKey

class PartitionMetadataSpec extends WavesSpec
  with PartitionMetadataFixture {

  def metadataWithoutAbsentKeys(metadata: () => PartitionMetadata) = {
    "not contain any absent paths" in {
      metadata().getAbsent shouldBe empty
    }
    "have no known absent keys" in {
      forAll (allKeys) { key =>
        metadata().isKnownAbsent(key) shouldBe (false)
      }
    }
    "allow all keys to be present" in {
      forAll (allKeys) { key =>
        metadata().canBePresent(key) shouldBe (true)
      }
    }
    "allow adding a present path" in {
      val myMetadata = metadata()
      myMetadata.addPresent(unrelated)
      myMetadata.getPresent should contain (unrelated)
      myMetadata.getPath.last should equal (Present)
    }
    "allow adding a path on the present side" in {
      val myMetadata = metadata()
      val present = myMetadata.getPresent.toSeq
      val absent = myMetadata.getAbsent.toSeq
      myMetadata.add(Present, unrelated)  // fixtures never contain the related key
      myMetadata.getPresent should contain theSameElementsAs (present :+ unrelated)
      myMetadata.getAbsent should contain theSameElementsAs (absent)
    }
  }
  def metadataWithoutPresentKeys(metadata: () => PartitionMetadata) = {
    "not contain any present paths" in {
      metadata().getPresent shouldBe empty
    }
    "have no known present keys" in {
      forAll (allKeys) { key =>
        metadata().isKnownPresent(key) shouldBe (false)
      }
    }
    "allow all keys to be absent" in {
      forAll (allKeys) { key =>
        metadata().canBeAbsent(key) shouldBe (true)
      }
    }
    "allow adding an absent path" in {
      val myMetadata = metadata()
      myMetadata.addAbsent(unrelated)
      myMetadata.getAbsent should contain (unrelated)
      myMetadata.getPath.last should equal (Absent)
    }
    "allow adding a path on the absent side" in {
      val myMetadata = metadata()
      val present = myMetadata.getPresent.toSeq
      val absent = myMetadata.getAbsent.toSeq
      myMetadata.add(Absent, unrelated)  // fixtures never contain the related key
      myMetadata.getPresent should contain theSameElementsAs (present)
      myMetadata.getAbsent should contain theSameElementsAs (absent :+ unrelated)
    }
  }
  def anyMetadata(metadata: () => PartitionMetadata) = {
    "be able to create an independant copy of itself" in {
      Given("a metadata object")
      val myMetadata = metadata()
      val originalPresent = myMetadata.getPresent
      val originalAbsent = myMetadata.getAbsent
      val originalPath = myMetadata.getPath

      When("we clone it")
      val copy = myMetadata.clone

      Then("the copy contains the same paths")
      copy.getAbsent should equal (originalAbsent)
      copy.getPresent should equal (originalPresent)

      And("when we modify the copy")
      copy.addAbsent(extension)
      copy.addPresent(prefix)

      Then("the original is unchanged")
      myMetadata.getAbsent should equal (originalAbsent)
      myMetadata.getPresent should equal (originalPresent)
      myMetadata.getPath should equal (originalPath)
    }
    "equal itself" in {
      metadata() should equal (metadata())
    }
    "not equal a different Metadata objects" in {
      val myMetadata = metadata()
      myMetadata shouldNot equal (PartitionMetadata(Seq.empty, Seq(unrelated), Seq.empty))
      myMetadata shouldNot equal (PartitionMetadata(Seq(unrelated), Seq.empty, Seq.empty))
      myMetadata shouldNot equal (PartitionMetadata(Seq.empty, Seq.empty, Seq(Rest)))
    }
    "not equal different objects" in {
      // yay for coverage
      metadata() should not equal (None)
    }
    "be able to add steps" in {
      val myMetadata = metadata()
      val path = myMetadata.getPath
      myMetadata.addStep(Rest)
      myMetadata.getPath should equal (path :+ Rest)
    }
    "not be a spill bucket" in {
      emptyMetadata should not be 'spillBucket
    }
  }

  "The PartitionMetadata" when {
    "it is empty" should {
      behave like metadataWithoutAbsentKeys(() => emptyMetadata)
      behave like metadataWithoutPresentKeys(() => emptyMetadata)
      behave like anyMetadata(() => emptyMetadata)

      "know no paths" in {
        forAll (allKeys) { key =>
          emptyMetadata.isKnown(key) shouldBe (false)
        }
        emptyMetadata.getPath shouldBe empty
      }
    }
    "it contains a known present path" should {
      behave like anyMetadata(() => emptyMetadata)
      behave like metadataWithoutAbsentKeys(() => presentMetadata)

      "contain only that present path" in {
        presentMetadata.getPresent should contain theSameElementsAs (Seq(thePathKey))
      }
      "know that path and all its prefixes are present" in {
        presentMetadata.isKnownPresent(prefix) shouldBe (true)
        presentMetadata.isKnownPresent(thePathKey) shouldBe (true)
        presentMetadata.isKnownPresent(extension) shouldBe (false)
        presentMetadata.isKnownPresent(unrelated) shouldBe (false)
      }
      "know that path and all its prefixes" in {
        presentMetadata.isKnown(prefix) shouldBe (true)
        presentMetadata.isKnown(thePathKey) shouldBe (true)
        presentMetadata.isKnown(extension) shouldBe (false)
        presentMetadata.isKnown(unrelated) shouldBe (false)
      }
      "not allow prefix to be absent" in {
        presentMetadata.canBeAbsent(prefix) shouldBe (false)
        presentMetadata.canBeAbsent(thePathKey) shouldBe (false)
        presentMetadata.canBeAbsent(extension) shouldBe (true)
        presentMetadata.canBeAbsent(unrelated) shouldBe (true)
      }
      "not allow adding prefixes as absent" in {
        an [IllegalArgumentException] shouldBe thrownBy (presentMetadata.addAbsent(prefix))
        an [IllegalArgumentException] shouldBe thrownBy (presentMetadata.addAbsent(thePathKey))
        presentMetadata.getAbsent shouldBe empty
      }
      "allow adding a longer path as absent" in {
        presentMetadata.addAbsent(extension)
        presentMetadata.getAbsent should contain theSameElementsAs (Seq(extension))
      }
      "allow adding unrelated paths as absent" in {
        presentMetadata.addAbsent(unrelated)
        presentMetadata.getAbsent should contain theSameElementsAs (Seq(unrelated))
      }
      "allow adding a prefix as present but filter it" in {
        presentMetadata.addPresent(prefix)
        presentMetadata.getPresent should contain theSameElementsAs (Seq(thePathKey))
      }
      "allow adding the path again but filter it" in {
        presentMetadata.addPresent(thePathKey)
        presentMetadata.getPresent should contain theSameElementsAs (Seq(thePathKey))
      }
    }
    "it contains a known absent key" should {
      behave like anyMetadata(() => emptyMetadata)
      behave like metadataWithoutPresentKeys(() => absentMetadata)

      "contain only that absent path" in {
        absentMetadata.getAbsent should contain theSameElementsAs (Seq(thePathKey))
      }
      "know that path and all its extensions are absent" in {
        absentMetadata.isKnownAbsent(prefix) shouldBe (false)
        absentMetadata.isKnownAbsent(thePathKey) shouldBe (true)
        absentMetadata.isKnownAbsent(extension) shouldBe (true)
        absentMetadata.isKnownAbsent(unrelated) shouldBe (false)
      }
      "know that path and all its extensions" in {
        absentMetadata.isKnown(prefix) shouldBe (false)
        absentMetadata.isKnown(thePathKey) shouldBe (true)
        absentMetadata.isKnown(extension) shouldBe (true)
        absentMetadata.isKnown(unrelated) shouldBe (false)
      }
      "not allow extenstions to be present" in {
        absentMetadata.canBePresent(prefix) shouldBe (true)
        absentMetadata.canBePresent(thePathKey) shouldBe (false)
        absentMetadata.canBePresent(extension) shouldBe (false)
        absentMetadata.canBePresent(unrelated) shouldBe (true)
      }
      "not allow adding extensions as present" in {
        an [IllegalArgumentException] shouldBe thrownBy (absentMetadata.addPresent(extension))
        an [IllegalArgumentException] shouldBe thrownBy (absentMetadata.addPresent(thePathKey))
        absentMetadata.getPresent shouldBe empty
      }
      "allow adding a prefix as present" in {
        absentMetadata.addPresent(prefix)
        absentMetadata.getPresent should contain theSameElementsAs (Seq(prefix))
      }
      "allow adding unrelated paths as present" in {
        absentMetadata.addPresent(unrelated)
        absentMetadata.getPresent should contain theSameElementsAs (Seq(unrelated))
      }
      "allow adding an extension as absent but filter it" in {
        absentMetadata.addAbsent(extension)
        absentMetadata.getAbsent should contain theSameElementsAs (Seq(thePathKey))
      }
      "allow adding the path again but filter it" in {
        absentMetadata.addAbsent(thePathKey)
        absentMetadata.getAbsent should contain theSameElementsAs (Seq(thePathKey))
      }
    }
    "its path points to a Spill bucket" should {
      "be a spill bucket" in {
        PartitionMetadata(Seq.empty, Seq.empty, Seq(Rest)) shouldBe 'spillBucket
      }
    }
  }
}

trait PartitionMetadataFixture extends BeforeAndAfterEach { this: Suite =>
  var emptyMetadata: PartitionMetadata = null
  var presentMetadata: PartitionMetadata = null
  var absentMetadata: PartitionMetadata = null

  val thePathKey = PathKey("a.b")
  val prefix = PathKey("a")
  val extension = PathKey("a.b.c")
  val unrelated = PathKey("b")
  val allKeys = Seq(prefix, thePathKey, extension, unrelated)

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    emptyMetadata = PartitionMetadata()
    presentMetadata = PartitionMetadata(Seq(thePathKey), Seq.empty, Seq(Present))
    absentMetadata = PartitionMetadata(Seq.empty, Seq(thePathKey), Seq(Absent))
  }
}