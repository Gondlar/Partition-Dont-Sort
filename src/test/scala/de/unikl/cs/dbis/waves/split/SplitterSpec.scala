package de.unikl.cs.dbis.waves.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture
import de.unikl.cs.dbis.waves.WavesTable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SplitterSpec extends WavesSpec
    with DataFrameFixture {

    "The Splitter" should {
        "read all data" in {
            TestSplitter().load(null).collect() should contain theSameElementsAs data
        }
        "access all data" in {
            TestSplitter().data(null).collect() should contain theSameElementsAs data
        }
        "pass the context to load" in {
            val splitter = new TestSplitter() {
                override def load(context: Any) = {
                    context should equal (5)
                    null
                }
            }
            splitter.data(5) should equal (null: DataFrame)
        }
    }
    

    case class TestSplitter() extends Splitter[Any] {
        override def partition() = ()
        override def load(context: Any): DataFrame = df
        override def data(context: Any): DataFrame = super.data(context)
    }
}
