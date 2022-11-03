package de.unikl.cs.dbis.waves.split

import de.unikl.cs.dbis.waves.util.operators.{Grouper,NullGrouper}
import org.apache.spark.sql.DataFrame

/**
  * A class to hold intermediate data between computation steps
  *
  * @param groups   the current grouped representation of the data
  * @param grouper  the grouper used to obtain groups
  * @param source   the source the groups were generated from
  */
final case class IntermediateData(
  groups: DataFrame,
  grouper: Grouper,
  source: DataFrame
) {

  /**
    * @return the DataFrame containing the raw data contained in this object
    */
  def toDF: DataFrame = grouper.find(groups, source)

  /**
    * Change the grouping of this data
    *
    * @param newGrouper the grouper to use
    * @return the IntermediateData object contaning the newly grouped data
    */
  def group(newGrouper: Grouper): IntermediateData = {
    val df = toDF
    IntermediateData(newGrouper.from(grouper, groups, df), newGrouper, df)
  }

  /**
    * @return true, iff this data is grouped, i.e., does not use the NullGrouper
    */
  def isGrouped: Boolean = grouper != NullGrouper

  /**
    * Apply the given transfrom to this data. This is a convenience function to
    * allow chaining.
    *
    * @param f the transform
    * @return  the result
    */
  def transform(f: IntermediateData => IntermediateData) = f(this)

  /**
    * @return the schema of the source data
    */
  def sourceSchema = source.schema

  /**
    * @return the schema of the current grouped representation
    */
  def groupSchema = groups.schema
}

object IntermediateData {
  def fromRaw(data: DataFrame) = IntermediateData(data, NullGrouper, data)
}