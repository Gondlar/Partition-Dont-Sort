package de.unikl.cs.dbis.waves.autosplit

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

trait ObjectCounters extends BeforeAndAfterEach { this: Suite =>


  var lhs : ObjectCounter = null
  var rhs : ObjectCounter = null

  override def beforeEach() {
    lhs = new ObjectCounter(Array(5, 3, 1))
    rhs = new ObjectCounter(Array(7, 1, 2))
    super.beforeEach() // To be stackable, must call super.beforeEach
  }

  override def afterEach() {}
}
