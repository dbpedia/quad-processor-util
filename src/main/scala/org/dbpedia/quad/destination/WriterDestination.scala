package org.dbpedia.quad.destination

import java.io.Writer

import org.dbpedia.quad.Quad
import org.dbpedia.quad.formatters.Formatter

/**
 * Writes quads to a writer.
 * 
 * @param called in open() to obtain the writer.
 */
class WriterDestination(factory: () => Writer, val formatter : Formatter)
extends Destination
{
  private var writer: Writer = null
  private var countQuads = 0
  
  override def open() = {
    if(writer == null) //to prevent errors when called twice
    {
      writer = factory()
      writer.write(formatter.header)
    }
  }
  
  /**
   * Note: using synchronization here is not strictly necessary (writers should be thread-safe),
   * but without it, different sequences of quads will be interleaved, which is harder to read
   * and makes certain parsing optimizations impossible.
   */
  override def write(graph : Traversable[Quad]) = synchronized {
    for(quad <- graph) {
      writer.write(formatter.render(quad))
      countQuads += 1
    }
  }

  override def close() = {
    if(writer != null) {
      writer.write(formatter.footer)
      System.err.println("Writing completed after " + countQuads + " quads")
      writer.close()
    }
  }

  def setHeader(head: String): Unit = formatter.setHeader(head)
  def setFooter(foot: String): Unit = formatter.setFooter(foot)
}
