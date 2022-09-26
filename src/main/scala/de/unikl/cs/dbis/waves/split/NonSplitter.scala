package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.util.operators.{Grouper,NullGrouper}

/**
  * Implements a splitter which does not split the data but delivers it as a
  * single partition
  */
class NonSplitter extends GroupedSplitter with FlatTreeBuilder {

  override protected def splitGrouper: Grouper = NullGrouper

  override protected def split(df: DataFrame): Seq[DataFrame] = Seq(df)
}
