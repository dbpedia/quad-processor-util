package org.dbpedia.quad.formatters

/**
 * Serialize quads to RDF/JSON
 */
class RDFJSONFormatter()
  extends TripleFormatter(() => new RDFJSONBuilder()) {

  override def header = ""

  override def footer = ""

  override def setHeader(head: String): Unit = ???

  override def setFooter(foot: String): Unit = ???
}
