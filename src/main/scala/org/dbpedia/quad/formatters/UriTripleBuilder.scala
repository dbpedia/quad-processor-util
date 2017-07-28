package org.dbpedia.quad.formatters

import java.net.{URI, URISyntaxException}

import org.dbpedia.quad.formatters.TripleBuilder._

abstract class UriTripleBuilder() extends TripleBuilder {
  
  protected val BadUri = "BAD URI: "
  
  def subjectUri(subj: String) = uri(subj, SUBJECT)
  
  def predicateUri(pred: String) = uri(pred, PREDICATE)
  
  def objectUri(obj: String) = uri(obj, OBJECT)
  
  def uri(uri: String, pos: Int): Unit
  
  protected def parseUri(str: String, pos: Int): String = {
    if (str == null) return BadUri+str
    try {
      var uri = new URI(str)
      if (! uri.isAbsolute()) return BadUri+"not absolute: "+str
      //if (policies != null) uri = policies(pos)(uri)
      uri.toString
    } catch {
      case usex: URISyntaxException =>
        BadUri+usex.getMessage() 
    }
  }
}