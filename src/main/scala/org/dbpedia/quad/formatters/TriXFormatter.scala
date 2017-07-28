package org.dbpedia.quad.formatters



/**
 * Formats statements according to the TriX format.
 * See: http://www.hpl.hp.com/techreports/2004/HPL-2004-56.html
 */
class TriXFormatter(quads: Boolean)
extends TripleFormatter(() => new TriXBuilder(quads))
{
  override def header = "<TriX xmlns=\"http://www.w3.org/2004/03/trix/trix-1/\" >\n"

  override def footer = "</TriX>\n"

  override def setHeader(head: String): Unit = ???

  override def setFooter(foot: String): Unit = ???
}
