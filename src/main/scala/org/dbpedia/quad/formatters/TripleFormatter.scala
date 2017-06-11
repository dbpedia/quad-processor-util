package org.dbpedia.quad.formatters

import org.dbpedia.quad.Quad

abstract class TripleFormatter(factory: () => TripleBuilder)
extends Formatter
{
  def render(quad: Quad): String = {
    
    val builder = factory()
    
    builder.start(quad.context)
    
    builder.subjectUri(quad.subject)
    
    builder.predicateUri(quad.predicate)
    
    if (quad.datatype == null) builder.objectUri(quad.value)
    else if (quad.datatype == "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString") builder.plainLiteral(quad.value, quad.language)
    else builder.typedLiteral(quad.value, quad.datatype)
    
    builder.end(quad.context)
    
    builder.result()
  }
}