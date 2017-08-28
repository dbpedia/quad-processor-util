package org.dbpedia.quad.destination

import java.io.Writer

import org.dbpedia.quad.Quad
import org.dbpedia.quad.formatters.Formatter

/**
 * Writes quads to a writer.
 * 
 * @param factory - called in open() to obtain the writer.
 */
class WriterDestination(factory: () => Writer, val formatter : Formatter)
extends Destination
{
  private var writer: Writer = _
  private var countQuads = 0
  private var tag: String = ""
  
  override def open(): Unit = {
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
  override def write(graph : Traversable[Quad]): Unit = synchronized {
    for(quad <- graph) {
      writer.write(formatter.render(quad))
      countQuads += 1
    }
  }

  override def close(): Unit = {
    if(writer != null) {
      writer.write(formatter.footer)
      writer.close()
    }
  }

  def setHeader(head: String): Unit = formatter.setHeader(head)
  def setFooter(foot: String): Unit = formatter.setFooter(foot)

  def getTag: String = this.tag
  def setTag(tag: String): Unit = this.tag = tag
}
