package de.unikl.cs.dbis.waves

import org.apache.hadoop.fs.{FileSystem,Path,FileUtil}
import scala.util.Random

class PartitionFolder(val baseDir: String, val name: String, var isTemporary: Boolean) extends Equals {
    private def tempFilename = baseDir + PartitionFolder.TEMP_DIR + '/' + name
    private def tempFile = new Path(tempFilename)
    private def finalFilename = baseDir + '/' + name
    private def finalFile = new Path(finalFilename)

    def filename = if (isTemporary) tempFilename else finalFilename
    def file = new Path(filename)
    override def toString(): String = filename

    override def canEqual(x : Any) = x.isInstanceOf[PartitionFolder]
    override def equals(x: Any): Boolean = x match {
        case other : PartitionFolder => baseDir == other.baseDir && name == other.name && isTemporary == other.isTemporary
        case _ => false
    }
    override def hashCode(): Int = {
        val prime = 251
        var result = 1
        result = prime * result + (if (isTemporary) 1 else 0)
        result = prime * result + baseDir.hashCode()
        result = prime * result + name.hashCode()
        result
    }

    def moveFromTempToFinal(fs: FileSystem) = {
        assert(isTemporary)
        fs.rename(tempFile, finalFile)
        isTemporary = false
    }

    /**
      * Moves the contents of the source here. If the source does not
      * exist, create an empty folder instead
      */
    def moveFrom(source: PartitionFolder, fs: FileSystem) = {
        if (source.exists(fs)) source.mv(fs, this) else mkdir(fs)
    }

    def copyContentsFrom(other: PartitionFolder, fs: FileSystem) = {
        assert(other != this)

        val it = fs.listFiles(other.file, false)
        val contents = Array.newBuilder[Path]
        while (it.hasNext()) {
            val file = it.next()
            if (file.isFile())
                contents += file.getPath()
        }
        FileUtil.copy(fs, contents.result(), fs, file, false, false, fs.getConf())
    }

    def mkdir(fs: FileSystem) = fs.mkdirs(file)
    def delete(fs: FileSystem) = fs.delete(file, true)
    def exists(fs: FileSystem) = {
        val loc = file
        fs.exists(loc) && fs.getFileStatus(loc).isDirectory()
    }
    def mv(fs: FileSystem, destination : PartitionFolder) = fs.rename(file, destination.file)

    def diskSize(fs: FileSystem) = fs.getContentSummary(file).getLength()
}

object PartitionFolder {
    val TEMP_DIR = "/tmp"

    def makeFolder(baseDir: String, temp: Boolean = true) = {
        val name = Random.nextLong().toHexString
        val partition = new PartitionFolder(baseDir, name, temp)
        partition
    }
}
