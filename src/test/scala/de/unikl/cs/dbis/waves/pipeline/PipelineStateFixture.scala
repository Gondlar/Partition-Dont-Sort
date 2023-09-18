package de.unikl.cs.dbis.waves.pipeline

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterEach

import de.unikl.cs.dbis.waves.DataFrameFixture

trait PipelineStateFixture extends BeforeAndAfterEach { this: Suite =>

  val dummyState = PipelineState(null, null)
  var dummyDfState: PipelineState = null

  override protected def beforeEach() = {
    super.beforeEach()
    this match {
      case withDf : DataFrameFixture
        => if (withDf.df != null) dummyDfState = PipelineState(withDf.df, null)
           else throw new RuntimeException("PipelineStateFixture must be added after DataFrameFixture")
      case _ => ()
    }
  }

}
