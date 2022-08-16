package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.{DataFrame => DataFrameFixture}

import org.apache.spark.sql.DataFrame

class SamplerSpec extends WavesSpec
    with DataFrameFixture {

    "The Sampler" should {

        // We cannot test the exact sampling amount because spark does not
        // guarantee exact numbers 
        "return a sample of the data" in {
            val sampler = TestSampler(.5)
            val samples = sampler.data(null).collect()
            sampler.calledSample should be (true)
            forAll (samples) { sample =>
                data.contains(sample) should be (true)
            }
        }
        "return nothing for a sample rate <= 0" in {
            TestSampler(0).data(null).count() should be (0)
            TestSampler(-10).data(null).count() should be (0)
        }
        "return the entire set for a sample rate >= 1" in {
            TestSampler(1).data(null).collect should contain theSameElementsAs (data)
            TestSampler(10).data(null).collect should contain theSameElementsAs (data)
        }
    }
    
    case class TestSampler(sampleRate: Double)
    extends MockSplitter with Sampler[Any] {
        var calledSample = false
        override protected def sampleRate(context: Any): Double = sampleRate
        override protected def sample(data: DataFrame, context: Any): DataFrame = {
            calledSample = true
            super.sample(data, context)
        }
        override def data(context: Any) = super.data(context)
    }

    class MockSplitter() extends Splitter[Any](null) {
        override def partition() = ()
        override def load(context: Any): DataFrame = df
    }
}
