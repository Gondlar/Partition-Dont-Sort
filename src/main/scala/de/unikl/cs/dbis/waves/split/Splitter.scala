package de.unikl.cs.dbis.waves.split

import de.unikl.cs.dbis.waves.WavesTable
import org.apache.spark.sql.DataFrame

/**
  * Base class for split Operations
  * 
  * The type parameter Context can be used to specify a data type which holds
  * information on the circumstances in which load is called. For example, the
  * [[RecursiveSplitter]] uses the context to specify the partition it is
  * currently working in and overrides [[load]] to only load that partition.
  *
  * @param table the table to split
  */
abstract class Splitter[Context] {

    /**
      * Automatically Partition the table
      */
    def partition(): Unit

    /**
      * Load the data from the data source.
      * 
      * Usually, this is the first step to access data. Use [[data]] to do so.
      *
      * @param context the context in which the data is loaded
      * @return the loaded data as a [[DataFrame]]
      */
    protected def load(context: Context): DataFrame

    /**
      * Access the data from the data source
      *
      * @param context the context in which the data is accessed
      * @return the data as a [[DataFrame]]
      */
    protected def data(context: Context): DataFrame = load(context)
}
