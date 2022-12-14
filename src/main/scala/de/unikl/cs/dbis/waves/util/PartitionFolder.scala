package de.unikl.cs.dbis.waves.util

import org.apache.hadoop.fs.{FileSystem,Path,FileUtil,RemoteIterator}
import java.util.UUID
import org.apache.spark.sql.SparkSession

/**
  * This class represents a folder which contains a bucket
  *
  * @param dBaseDir the parent directory of this folder, i.e., the waves directory
  * @param dName this folder's name
  * @param temporary whether this folder is temporary. Temporary folders are not
  *                  located directly in their parent directory but in a
  *                  specific subfolder
  */
class PartitionFolder(
    private var dBaseDir: String,
    private var dName: String,
    private var temporary: Boolean
) extends Equals {
    import PartitionFolder.RemoteWrapper

    /**
      * @return this folder's base directory
      */
    def baseDir = dBaseDir

    /**
      * @return this folder's name
      */
    def name = dName

    /**
      * @return whether this folder is temporary
      */
    def isTemporary = temporary

    private def tempFilename = s"$dBaseDir/${PartitionFolder.TEMP_DIR}/$dName"
    private def finalFilename = s"$dBaseDir/$dName"

    /**
      * @return the path to this folder
      */
    def filename = if (temporary) tempFilename else finalFilename

    /**
      * @return the path to this folder using the HDFS class
      */
    def file = new Path(filename)

    override def toString(): String = filename

    override def canEqual(x : Any) = x.isInstanceOf[PartitionFolder]
    override def equals(x: Any): Boolean = x match {
        case other : PartitionFolder =>
            other.canEqual(this) && dBaseDir == other.dBaseDir && dName == other.dName && temporary == other.temporary
        case _ => false
    }
    override def hashCode(): Int = {
        val prime = 251
        var result = 1
        result = prime * result + temporary.hashCode()
        result = prime * result + dBaseDir.hashCode()
        result = prime * result + dName.hashCode()
        result
    }

    /**
      * Copy all files from the given folder to this folder
      *
      * @param other the folder to copy from
      * @param fs the filesystem both folders are located on
      */
    def copyContentsFrom(other: PartitionFolder)(implicit fs: FileSystem) = {
        assert(other != this)

        val contents = fs.listFiles(other.file, false).map(_.getPath).toArray
        FileUtil.copy(fs, contents, fs, file, false, false, fs.getConf())
    }

    /**
      * Create this folder on the filesystem
      *
      * @param fs the filesystem this folder is located in
      */
    def mkdir(implicit fs: FileSystem) = fs.mkdirs(file)

    /**
      * Delete this folder and all of its contents from the filesystem
      *
      * @param fs the filesystem this folder is located in
      */
    def delete(implicit fs: FileSystem) = fs.delete(file, true)

    /**
      * Check whether this folder exists on the filesystem
      *
      * @param fs the filsystem this folder is located in (if it exists)
      * @return true iff the folder exists
      */
    def exists(implicit fs: FileSystem) = {
        val loc = file
        fs.exists(loc) && fs.getFileStatus(loc).isDirectory()
    }

    /**
      * Move this folder to the new location specified by the parameters. All
      * parameters default to the current values of the respective attributes.
      * Use named parameters to change specific characteristics.
      *
      * @param newBaseDir the baseDir to move to
      * @param newName the new name for this folder
      * @param newTemporary the new temporary status
      * @param fs the filesystem this folder is located in
      */
    def mv(
        newBaseDir: String = dBaseDir,
        newName: String = dName,
        newTemporary: Boolean = temporary
    )(implicit fs: FileSystem) = {
        val oldFile = file
        dBaseDir = newBaseDir
        dName = newName
        temporary = newTemporary
        val newFile = file
        fs.rename(oldFile, newFile)
    }

    /**
      * rename this folder
      *
      * @param newName the new name
      * @param fs the filesystem this folder is located in
      */
    def rename(newName: String)(implicit fs: FileSystem) = mv(newName = newName)

    /**
      * If this folder is currently temporary, move it out of the temp dir and
      * make it non-temporary. Otherwise, make it temporary and move it into the
      * temp dir.
      *
      * @param fs the filesystem this folder is located in
      */
    def moveBetweenTemp(implicit fs: FileSystem) = mv(newTemporary = !temporary)

    /**
      * The size of this folder's contents on disk in bytes
      *
      * @param fs the filesystem this folder is located in
      * @return the folder's size
      */
    def diskSize(implicit fs: FileSystem) = fs.getContentSummary(file).getLength()

    /**
      * List all Parquet files in this folder
      *
      * @param fs the filesystem this folder is located in
      * @return An iterator over all paths to Parquet files
      */
    def parquetFiles(implicit fs: FileSystem): Iterator[Path]
      = fs.listFiles(file, false)
        .map(_.getPath)
        .filter(_.getName() endsWith ".parquet")

    /**
      * Check if the folder is empty, i.e., does not contain any Parquet files.
      * A folder with 100 files none of which are Parquet files is considered
      * empty! Parquet files are identified by their extension.
      *
      * @param fs the filesystem this folder is located in
      * @return true iff the folder is empty
      */
    def isEmpty(implicit fs: FileSystem): Boolean
        = !parquetFiles.hasNext

    /**
      * Find the filesystem which contains this folder
      *
      * @param spark the current spark session
      * @return the filesystem
      */
    def filesystem(spark: SparkSession)
      = file.getFileSystem(spark.sparkContext.hadoopConfiguration)
}

object PartitionFolder {

    /**
      * the name of the subdiectory for temporary folders
      */
    val TEMP_DIR = "tmp"

    /**
      * Return a folder with a random name. The folder is not created in the
      * filesysstem yet.
      *
      * @param baseDir the new folders base directory
      * @param temp whether the new folder should be temporary
      * @return a new folder with a random name
      */
    def makeFolder(baseDir: String, temp: Boolean = true)
        = new PartitionFolder(baseDir, UUID.randomUUID().toString(), temp)

    implicit class RemoteWrapper[T](remote: RemoteIterator[T]) extends Iterator[T] {
        override def hasNext: Boolean = remote.hasNext()
        override def next(): T = remote.next()
    }

    /**
      * Find all PartitionFolders in a given directory
      *
      * @param baseDir the path to the directory
      * @param fs the filesystem the directory is located in
      * @return an iterator over all PartitionFolders in that directory
      */
    def allInDirectory(baseDir: Path)(implicit fs: FileSystem) = {
      fs.listStatus(baseDir).iterator
        .filter(file => file.isDirectory() && file.getPath.getName() != TEMP_DIR)
        .map(file => new PartitionFolder(baseDir.toString, file.getPath.getName(), false))
    }
}
