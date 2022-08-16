package de.unikl.cs.dbis.waves.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.Relation
import de.unikl.cs.dbis.waves.WavesTable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SplitterSpec extends WavesSpec
    with Relation {

    "The Splitter" should {
        "read all data" in {
            val table = WavesTable("Test Table", spark, directory, CaseInsensitiveStringMap.empty(), schema)
            TestSplitter(table).load(null).collect() should contain theSameElementsAs data
        }
        "access all data" in {
            val table = WavesTable("Test Table", spark, directory, CaseInsensitiveStringMap.empty(), schema)
            TestSplitter(table).data(null).collect() should contain theSameElementsAs data
        }
        "pass the context to load" in {
            val splitter = new TestSplitter(null) {
                override def load(context: Any) = {
                    context should equal (5)
                    null
                }
            }
            splitter.data(5) should equal (null: DataFrame)
        }
    }
    

    case class TestSplitter(table: WavesTable) extends Splitter[Any](table) {
        override def partition() = ()
        override def load(context: Any): DataFrame = super.load(context)
        override def data(context: Any): DataFrame = super.data(context)
    }
}
