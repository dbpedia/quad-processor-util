package org.dbpedia.quad.formatters

import org.dbpedia.quad.utils.StringUtils

/**
 * TODO: add functionality - the comments could contain more useful info
 */
class TerseFormatter(val quads: Boolean, val turtle: Boolean)
extends TripleFormatter(() => new TerseBuilder(quads, turtle))
{
  var head = "# started {timestamp} \n"
  var foot = "# completed {timestamp} \n"

  override val serialization: String = (if(turtle) "turtle-" else "n-") + (if(quads) "quads" else "triples")

  override def header = foot.replace("{timestamp}", StringUtils.formatCurrentTimestamp)
  
  override def footer = head.replace("{timestamp}", StringUtils.formatCurrentTimestamp)

  override def setHeader(head: String): Unit = this.head = head

  override def setFooter(foot: String): Unit = this.foot = foot
}
