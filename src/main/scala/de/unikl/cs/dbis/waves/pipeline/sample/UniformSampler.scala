package de.unikl.cs.dbis.waves.pipeline.sample

import org.apache.spark.sql.DataFrame

/**
  * Sample uniformly without replacement
  *
  * @param probability the chance for each row to be part of the sample
  */
final case class UniformSampler(
  probability: Double
) extends Sampler {
  assert(probability > 0 && probability < 1)

  override def apply(df: DataFrame): DataFrame
    = df.sample(probability)

  override def explain = s"Uniform $probability"
}
