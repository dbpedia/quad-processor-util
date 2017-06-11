package org.dbpedia.quad.destination

import org.dbpedia.quad.Quad

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
}
