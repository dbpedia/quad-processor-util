package org.dbpedia.quad.destination

import org.dbpedia.quad.Quad
import org.dbpedia.quad.formatters.Formatter

import scala.collection.mutable.LinkedHashSet

/**
 */
class DeduplicatingDestination(destination: Destination)
extends WrapperDestination(destination)
{
    override def write(graph : Traversable[Quad]) = {
      // use LinkedHashSet to preserve order
      val unique = new LinkedHashSet[Quad]
      unique ++= graph
      super.write(unique)
    }

  /**
    * provide information about the intended format (syntax) of the destination file
    */
  override val formatter: Formatter = destination.formatter
}
