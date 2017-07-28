package org.dbpedia.quad.destination

import org.dbpedia.quad.Quad
import org.dbpedia.quad.formatters.Formatter

/**
 * A destination for RDF quads.
 */
trait Destination
{
  /**
    * provide information about the intended format (syntax) of the destination file
    */
  val formatter: Formatter

  /**
   * Opens this destination. This method should only be called once during the lifetime
   * of a destination, and it should not be called concurrently with other methods of this class.
   */
  def open(): Unit

  /**
   * Writes quads to this destination. Implementing classes should make sure that this method
   * can safely be executed concurrently by multiple threads.
   */
  def write(graph : Traversable[Quad]): Unit

  /**
   * Closes this destination. This method should only be called once during the lifetime
   * of a destination, and it should not be called concurrently with other methods of this class.
   */
  def close(): Unit
}
