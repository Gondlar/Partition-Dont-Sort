package de.unikl.cs.dbis.waves.util

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.util.functional._

class FunctionalSpec extends WavesSpec {

  def f(x: Int) = x+5
  def g(x: Int) = x*2
  def h(x: Int) = x*x

  "The pipeline operator" should {
    "chain functions" when {
      "there are two of them" in {
        (3 |> f) should equal (8)
      }
      "there are multiple" in {
        (3 |> f |> g |> h) should equal (256)
      }
    }
  }
}