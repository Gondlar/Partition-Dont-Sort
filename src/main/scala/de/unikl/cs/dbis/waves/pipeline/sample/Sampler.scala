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
}

/**
  * Do not sample
  */
object NullSampler extends Sampler {
  override def apply(df: DataFrame): DataFrame = df
}
