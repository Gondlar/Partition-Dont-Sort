package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec

class PartitionTreePathSpec extends WavesSpec {

  "A PartitionTreePath" should {
    "print the correct class name" in {
      println(Present)
      Present.toString() should equal ("Present")
      Absent.toString() should equal ("Absent")
      Partitioned.toString() should equal ("Partitioned")
      Rest.toString() should equal ("Rest")
      Less.toString() should equal ("Less")
      MoreOrNull.toString() should equal ("MoreOrNull")
    }
  }
}