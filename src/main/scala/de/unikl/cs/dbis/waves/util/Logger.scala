package de.unikl.cs.dbis.waves.util

import java.nio.charset.StandardCharsets
import java.util.Calendar
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import java.text.SimpleDateFormat

object Logger {
    val logDir = "log"

    protected case class Event(name: String, time: Long, payload: String) {
        override def toString() = s"$time,\"$name\",\"$payload\""
    }

    private val events = ListBuffer.empty[Event]

    private def now = Calendar.getInstance().getTime()
    private val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")

    def log(name : String, payload: Any = "") = {
        events.addOne(Event(name, now.getTime(), payload.toString))
    }

    override def toString(): String = events.mkString("\n")

    def clear() = events.clear()

    def printToStdout() = println(this)

    def writeToDisk(config : Configuration) = {
        val path = new Path(s"$logDir/log-${dateFormat.format(now)}.csv")
        val fs = path.getFileSystem(config)
        val out = fs.create(path)
        out.write(toString().getBytes(StandardCharsets.UTF_8))
        out.close()
    }

    def flush(config : Configuration) = {
        printToStdout()
        writeToDisk(config)
        clear()
    }
  
}