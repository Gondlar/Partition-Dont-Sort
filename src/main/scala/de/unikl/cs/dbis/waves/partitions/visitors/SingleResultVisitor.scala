package de.unikl.cs.dbis.waves.partitions.visitors

/**
  * Mixin for Visitors which only produce one result
  */
trait SingleResultVisitor[-Payload,+Result] extends PartitionTreeVisitor[Payload] {

  /**
    * @return the result
    */
  def result: Result
}
