package de.unikl.cs.dbis.waves.testjobs

import de.unikl.cs.dbis.waves.split.recursive.RowwiseCalculator
import de.unikl.cs.dbis.waves.util.PathKey

object Distributions {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args :+ "master=local" :+ "inputPath=/home/patrick/Datasets/yelp/yelp_academic_dataset_business.json")
    val spark = jobConfig.makeSparkSession(s"Distributions ${jobConfig.inputPath}")

    val df = spark.read.json(jobConfig.inputPath)
    // spark infers all-optional schemas, so the following will check all nodes
    val calc = RowwiseCalculator()
    val (total, presentCount, _) = calc.calcRaw(df)
    val allPaths = calc.paths(df).toSeq
    val counts = presentCount.toMap(allPaths)
    
    val leaves = allPaths.foldRight(Seq.empty[PathKey])((path, rest) => {
      if (rest.exists(path isPrefixOf _)) rest else path +: rest 
    })
    var partsum = 0
    val probabilities = for (column <- leaves) yield {
      val amounts = Seq.iterate(column,column.maxDefinitionLevel)(_.parent.get).map(counts(_)).reverse
      val optionalPresence = amounts.foldLeft(Seq(total))({case (list@(x::_), next) =>
        if (x == next) list else next::list
      })
      if (optionalPresence.size == 1) None else {
        val frequencies = (optionalPresence.head +: optionalPresence.sliding(2).map({case smaller::larger::_ => larger-smaller}).toSeq).reverse
        assert(frequencies.sum == total)
        val agg = total+frequencies.zipWithIndex.map{ case (amount, dl) => amount * dl}.sum
        partsum += agg
        Some(total.toDouble / agg)
      }
    }
    val sorted = probabilities.flatten.sorted
    println(sorted)
    println(s"Average: ${sorted.sum / sorted.length}")
    println(s"Median: ${sorted(sorted.size/2)}")
    println(s"One Estimate: ${(total.toDouble * leaves.size) / partsum}")
  }
}
