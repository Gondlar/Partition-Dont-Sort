package de.unikl.cs.dbis.waves.util

import java.util.Calendar
import scala.collection.mutable.ListBuffer

object Logger {

    protected case class Event(name: String, time: Long, payload: String) {
        override def toString() = s"$time,\"$name\",\"$payload\""
    }

    private val events = ListBuffer.empty[Event]

    def log(name : String, payload: Any = "") = {
        val now = Calendar.getInstance()
        val timestamp = now.getTime().getTime()
        events.addOne(Event(name, timestamp, payload.toString))
    }

    def printToStdout() = {
        println(events.mkString("\n"))
    }
  
}