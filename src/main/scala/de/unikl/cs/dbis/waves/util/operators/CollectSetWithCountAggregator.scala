package de.unikl.cs.dbis.waves.util.operators

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders

import scala.collection.mutable.Map
import scala.reflect.runtime.universe.TypeTag

/**
  * Aggregator to collect a List of both distinct values and their respective count
  */
class CollectSetWithCountAggregator[Key : TypeTag]
extends Aggregator[Key,Map[Key,Long],Seq[(Key,Long)]] {

  override def zero: Map[Key,Long] = Map.empty

  override def reduce(map: Map[Key,Long], elem: Key): Map[Key,Long] = {
    if (elem == null) return map
    increaseCount(map, elem)
  }

  override def merge(lhs: Map[Key,Long], rhs: Map[Key,Long]): Map[Key,Long] = {
    for ((elem, count) <- rhs) {
      increaseCount(lhs, elem, count)
    }
    lhs
  }

  private def increaseCount(map: Map[Key,Long], elem: Key, count: Long = 1) = {
    map.put(elem,map.get(elem).map(_+count).getOrElse(count))
    map
  }

  override def finish(map: Map[Key,Long]): Seq[(Key, Long)]
    = map.toSeq

  override def bufferEncoder: Encoder[Map[Key,Long]] = Encoders.kryo

  override def outputEncoder: Encoder[Seq[(Key, Long)]] = ExpressionEncoder()
}
