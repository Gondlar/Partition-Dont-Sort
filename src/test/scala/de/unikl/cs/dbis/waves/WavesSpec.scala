package de.unikl.cs.dbis.waves

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should._
import org.scalatest.GivenWhenThen
import org.scalatest.EitherValues

abstract class WavesSpec extends AnyWordSpec
    with Matchers
    with GivenWhenThen
    with EitherValues
