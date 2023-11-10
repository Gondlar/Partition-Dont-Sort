package de.unikl.cs.dbis.waves.partitions

import java.nio.charset.StandardCharsets
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

/**
  * Class to Interface [[read]] and [[write]] [[PartitionTree]]s from HDFS
  *
  * @param fs the fs the tree resides on
  * @param schemaPath the path to the JSON file this interface is used for
  */
class PartitionTreeHDFSInterface private (
  val fs: FileSystem,
  schemaPath: Path
) {

  /**
    * write the given tree to the file system
    *
    * @param tree the tree to write
    */
  def write(tree: PartitionTree[String]) : Unit = {
    val json = tree.toJson.getBytes(StandardCharsets.UTF_8)
    val out = fs.create(schemaPath)
    out.write(json)
    out.close()
  }

  /**
    * read the tree from the file system
    *
    * @return None if the file does not exist, otherwise the tree
    */
  def read() : Option[PartitionTree[String]] = {
    if (!fs.exists(schemaPath)) return None

    val res = ArrayBuffer.empty[Byte]
    val buffer = new Array[Byte](256*256) // Possibly use differing size
    val reader = fs.open(schemaPath)
    var read = 0
    while ({read = reader.read(buffer); read != -1}) {
        res ++= buffer.slice(0, read)
    }
    Some(PartitionTree.fromJson(new String(res.toArray, StandardCharsets.UTF_8)))
  }
}

object PartitionTreeHDFSInterface {

  /**
    * Derive the path to the schema file from the base path
    *
    * @param basePath the path to the root directory of the table
    * @return the path to the file
    */
  private def schemaPath(basePath: String) = s"$basePath/schema.json"

  /**
    * Construct a PartitionTreeHDFSInterface from a SparkSession and a base path
    * We use the spark session and the path to automatically find the correct file system
    *
    * @param spark the spark session 
    * @param basePath the path to the root directory of the table
    * @return the created PartitionTreeHDFSInterface
    */
  def apply(spark: SparkSession, basePath: String) : PartitionTreeHDFSInterface
    = withExactLocation(spark, schemaPath(basePath))

  /**
    * Construct a PartitionTreeHDFSInterface from a FileSystem and a base path
    *
    * @param fs the file system
    * @param basePath the path to the root directory of the table
    * @return the created PartitionTreeHDFSInterface
    */
  def apply(fs: FileSystem, basePath: String) : PartitionTreeHDFSInterface
    = withExactLocation(fs, schemaPath(basePath))

  /**
    * Construct a PartitionTreeHDFSInterface from a SparkSession and a path to
    * the schema.  We use the spark session and the path to automatically find
    * the correct file system.
    *
    * @param spark the spark session
    * @param path the path to the root directory of the table
    * @return the created PartitionTreeHDFSInterface
    */
  def withExactLocation(spark: SparkSession, path: String) = {
    val mySchemaPath = new Path(path)
    val fs = mySchemaPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    new PartitionTreeHDFSInterface(fs, mySchemaPath)
  }

  /**
    * Construct a PartitionTreeHDFSInterface from a FileSystem and a path
    *
    * @param fs the file system 
    * @param path the path to the root directory of the table
    * @return the created PartitionTreeHDFSInterface
    */
  def withExactLocation(fs: FileSystem, path: String)
    = new PartitionTreeHDFSInterface(fs, new Path(path))
}
