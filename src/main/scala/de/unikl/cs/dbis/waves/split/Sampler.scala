package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

/**
  * Mixin for [[Splitter]] which samples the data when loading
  */
trait Sampler[Context] extends Splitter[Context] {

    /**
      * Specify the sample rate
      * 
      * If a value of 1 or greater is returned, sampling is skipped. If a value
      * of 0 or less is returned, an empty sample is returned.
      *
      * @param context the context under which sampling happens
      * @return the sample right as a double.
      */
    protected def sampleRate(context: Context): Double

    /**
      * Samples data according to [[sampleRate]]
      *
      * @param data the data
      * @param context the context under which sampling happens
      * @return the sampled data
      */
    protected def sample(data: DataFrame, context: Context): DataFrame = {
        val rate = sampleRate(context)
        if (rate <= 0) {
            data.sparkSession.createDataFrame(
                data.sparkSession.sparkContext.emptyRDD[Row],
                data.schema
            )
        } else if (rate < 1) data.sample(rate)
        else data
    }

    override protected def data(context: Context) = sample(super.data(context), context)
}
