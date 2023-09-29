package de.unikl.cs.dbis.waves.testjobs

import de.unikl.cs.dbis.waves.partitions._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.visitors.PartitionTreeVisitor
import de.unikl.cs.dbis.waves.partitions.visitors.SingleResultVisitor
import de.unikl.cs.dbis.waves.util.PathKey

import org.apache.hadoop.fs.Path
import java.nio.charset.StandardCharsets

object FindBadSplits {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession(s"FindBadSplits ${jobConfig.wavesPath}")

    val hdfs = PartitionTreeHDFSInterface(spark, jobConfig.wavesPath)
    implicit val fs = hdfs.fs

    val tree = hdfs.read().get
    val visitor = new PartitionTreeVisitor[String] with SingleResultVisitor[String,Seq[Seq[(PartitionTreePath, PathKey)]]] {

      var currentNodeEmpty = false
      var knownEmpty: Seq[Seq[(PartitionTreePath, PathKey)]] = Seq.empty

      override def visit(bucket: Bucket[String]): Unit = {
        currentNodeEmpty = bucket.folder(jobConfig.wavesPath).isEmpty
      }

      override def visit(node: SplitByPresence[String]): Unit = {
        node.absentKey.accept(this)
        val absentEmpty = currentNodeEmpty
        val absentKnown = knownEmpty.map((Absent, node.key) +: _)
        knownEmpty = Seq.empty

        node.presentKey.accept(this)
        val presentEmpty = currentNodeEmpty
        val presentKnown = knownEmpty.map((Present, node.key) +: _)

        val newKnown = (absentEmpty, presentEmpty) match {
          case (true, false) => Seq(Seq((Absent, node.key)))
          case (false, true) => Seq(Seq((Present, node.key)))
          case _ => Seq.empty
        }
        currentNodeEmpty = presentEmpty && absentEmpty
        knownEmpty = newKnown ++ absentKnown ++ presentKnown
      }

      override def visit(node: SplitByValue[String]): Unit = {
        node.less.accept(this)
        val lessEmpty = currentNodeEmpty
        val lessKnown = knownEmpty.map((Less, node.key) +: _)
        knownEmpty = Seq.empty

        node.more.accept(this)
        val moreEmpty = currentNodeEmpty
        val moreKnown = knownEmpty.map((MoreOrNull, node.key) +: _)

        val newKnown = (lessEmpty, moreEmpty) match {
          case (true, false) => Seq(Seq((Less, node.key)))
          case (false, true) => Seq(Seq((MoreOrNull, node.key)))
          case _ => Seq.empty
        }
        currentNodeEmpty = lessEmpty && moreEmpty
        knownEmpty = newKnown ++ lessKnown ++ moreKnown
      }

      override def visit(root: Spill[String]): Unit = ???

      override def result: Seq[Seq[(PartitionTreePath, PathKey)]]
        = knownEmpty
    }
    val output = tree.root(visitor).sortBy(_.size).reverse.mkString("\n")

    val path = new Path(s"badPaths.txt")
    val out = fs.create(path)
    out.write(output.getBytes(StandardCharsets.UTF_8))
    out.close()
  }
}
