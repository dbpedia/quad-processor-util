package org.dbpedia.quad.formatters

import org.dbpedia.quad.formatters.UriPolicy._

/**
 * Serialize quads to RDF/JSON
 */
class RDFJSONFormatter(policies: Array[Policy] = null)
  extends TripleFormatter(() => new RDFJSONBuilder(policies)) {

  override def header = ""

  override def footer = ""

}
