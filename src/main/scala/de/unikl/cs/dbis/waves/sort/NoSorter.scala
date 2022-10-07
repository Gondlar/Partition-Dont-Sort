package de.unikl.cs.dbis.waves.sort

import org.apache.spark.sql.DataFrame

import de.unikl.cs.dbis.waves.util.operators.Grouper
import de.unikl.cs.dbis.waves.util.operators.NullGrouper

/**
  * Sorter for when we do not wish to sort
  */
object NoSorter extends Sorter {

  override def sort(bucket: DataFrame): DataFrame = bucket

  override def grouper: Grouper = NullGrouper

}
