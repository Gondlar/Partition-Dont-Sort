package de.unikl.cs.dbis.waves.split

import de.unikl.cs.dbis.waves.WavesTable
import de.unikl.cs.dbis.waves.sort.Sorter
import org.apache.spark.sql.DataFrame

/**
  * Base class for split Operations
  * 
  * The type parameter Context can be used to specify a data type which holds
  * information on the circumstances in which load is called. For example, the
  * [[RecursiveSplitter]] uses the context to specify the partition it is
  * currently working in and overrides [[load]] to only load that partition.
  */
abstract class Splitter[Context] {

    /**
      * Set the given DataFrame as the one to be split. You may not call
      * methods on a Splitter befre a DataFrame has been prepared.
      *
      * @param df the DataFrame
      * @param path the path the result should be written to
      * @return this splitter for chaining
      */
    def prepare(df: DataFrame, path: String): Splitter[Context]

    /**
      * @return Whether prepare has been called on the Splitter
      */
    def isPrepared: Boolean

    /**
      * Get the path this splitter writes to
      *
      * @return the path
      */
    def getPath: String

    /**
      * throws an IllegalStateException if this splitter is not prepared
      */
    protected def assertPrepared
      = if (!isPrepared) throw new IllegalStateException("Splitter was not prepared")

    /**
      * Sort the resulting partitions. The default value depends on the specific
      * splitter. Some splitters may reject some or all sorters.
      *
      * @param sorter the sorter to use
      * @return this splitter for chaining
      * @throws IllegalArgumentException if the given sorter is unsupported
      */
    def sortWith(sorter: Sorter): Splitter[Context]

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
