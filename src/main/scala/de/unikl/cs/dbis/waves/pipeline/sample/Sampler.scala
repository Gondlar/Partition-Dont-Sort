package de.unikl.cs.dbis.waves.pipeline.sample

import org.apache.spark.sql.DataFrame

/**
  * An interface for sampling input data
  */
trait Sampler {

  /**
    * Sample the given data
    *
    * @param df the input data
    * @return the DataFrame containing the sample
    */
  def apply(df: DataFrame): DataFrame

  /**
    * A human readable summary of the kind of Sampler and its parameters
    *
    * @return
    */
  def explain : String
}

/**
  * Do not sample
  */
object NullSampler extends Sampler {  
  override def apply(df: DataFrame): DataFrame = df
  override val explain: String = "Off"
}
