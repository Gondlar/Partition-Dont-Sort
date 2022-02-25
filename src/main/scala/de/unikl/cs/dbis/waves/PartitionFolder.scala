package de.unikl.cs.dbis.waves

import org.apache.hadoop.fs.{FileSystem,Path}
import scala.util.Random
import java.io.IOException

class PartitionFolder(val baseDir: String, val name: String, var isTemporary: Boolean) {
    private def tempFilename = baseDir + PartitionFolder.TEMP_DIR + '/' + name
    private def tempFile = new Path(tempFilename)
    private def finalFilename = baseDir + '/' + name
    private def finalFile = new Path(finalFilename)

    def filename = if (isTemporary) tempFilename else finalFilename
    def file = new Path(filename)
    override def toString(): String = filename

    def moveFromTempToFinal(fs: FileSystem) = {
        assert(isTemporary)
        fs.rename(tempFile, finalFile)
        isTemporary = false
    }

    def delete(fs: FileSystem) = fs.delete(file, true)
}

object PartitionFolder {
    val TEMP_DIR = "/tmp"

    def makeFolder(baseDir: String, fs: FileSystem, temp: Boolean = true) = {
        val name = Random.nextLong().toHexString
        val partition = new PartitionFolder(baseDir, name, temp)
        partition
    }
}