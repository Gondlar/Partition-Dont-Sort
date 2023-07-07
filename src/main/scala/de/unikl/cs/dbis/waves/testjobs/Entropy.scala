package de.unikl.cs.dbis.waves.testjobs

import de.unikl.cs.dbis.waves.split.recursive.RowwiseCalculator
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.operators.DefinitionLevelGrouper
import org.apache.spark.sql.Row
import de.unikl.cs.dbis.waves.split.recursive.ObjectCounter
import de.unikl.cs.dbis.waves.util.nested.schemas._
import de.unikl.cs.dbis.waves.split.recursive.PrecalculatedMetric
import de.unikl.cs.dbis.waves.split.recursive.Metric
import collection.JavaConverters._
import scala.annotation.meta.field
import scala.annotation.strictfp
import org.apache.spark.sql.types.StructType
import Math.log10
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface

object Entropy {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args :+ "master=local" :+ "inputPath=/home/patrick/Datasets/yelp/yelp_academic_dataset_business.json")
    val spark = jobConfig.makeSparkSession(s"Distributions ${jobConfig.inputPath}")

    val df = spark.read.json(jobConfig.inputPath)
    val total = df.count()
    val dls = df.select(DefinitionLevelGrouper(df.schema))
    val pathsSeq = paths(df.schema)
    val pathMap = pathsSeq.zipWithIndex.toMap
    val runs = dls.rdd
      .mapPartitions(countRuns(df.schema.leafCount))
      .reduce((lhs, rhs) => {
        lhs += rhs
        lhs
      }).toMap(pathsSeq).toSeq
      
    for ((path, runs) <- runs) {
      val dlColumn = DefinitionLevelGrouper.GROUP_COLUMN(pathMap(path))
      val counts = dls.groupBy(dlColumn).count()
        .collect().toSeq
      assert(counts.map(_.getLong(1)).sum == total)
      val probs = counts.map(row => (row.getInt(0), row.getLong(1).toDouble/total))
      // Gini coefficient as commonly used
      val gini = 1 - probs.map({case (_, prob) => prob*prob}).sum
      // shannon entropy
      val entropy = - probs.map({case (_, prob) => prob * log10(prob)/log10(2)}).sum
      // weighted entropy as introduced by Mukherjee et al.: Towards Optimizing Storage Costs on the Cloud. ICDE 23
      // length of an int is its number of digits in base 10 plus two characters for newline in CSV
      val weightedEntropy = probs.map({case (level, prob) => (level.toString.length+2) * prob * log10(prob)/log10(2)}).sum

      val folder = new PartitionFolder("/tmp", "sizetest", false)
      implicit val fs = PartitionTreeHDFSInterface(df.sparkSession, folder.filename).fs
      dls.select(dlColumn).write.option("compression", "gzip").csv("/tmp/sizetest")
      val gzipSize = folder.diskSize
      folder.delete
      dls.select(dlColumn).write.option("compression", "none").csv("/tmp/sizetest")
      val uncompressedSize = folder.diskSize
      folder.delete
      
      println(s"$path, $runs, $gini, $entropy, $weightedEntropy, $uncompressedSize, $gzipSize")
    }
  }

  def countRuns(optionalNodeCount: Int)(partition: Iterator[Row]): Iterator[ObjectCounter] = {
    val runCounts = ObjectCounter(optionalNodeCount)
    
    if (partition.isEmpty)
      return Iterator(runCounts)

    var oldRow = partition.next().getList[Int](0).asScala
    runCounts += 1
    for (row <- partition) {
      val current = row.getList[Int](0).asScala
      runCounts <-- new Metric() {
        override def measure(results: Array[Int]): Unit = {
          assert(results.length == oldRow.length)
          assert(results.length == current.length)
          for (i <- results.indices if oldRow(i) != current(i)) {
            results(i) += 1
          }
        }
      }
      oldRow = current
    }

    return Iterator(runCounts)
  }

  def paths(schema : StructType) : Seq[PathKey]
    = schema.fields.flatMap(field => {
        val name = field.name
        field.dataType match {
          case struct : StructType => paths(struct).map(name +: _)
          case _ => Seq(PathKey(name))
        }
      })
}
