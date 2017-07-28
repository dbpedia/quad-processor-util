package org.dbpedia.quad.formatters

/**
 * Helps to render one triple/quad.
 * 
 * Objects of this class are not re-usable - create a new object for each triple.
 */
trait TripleBuilder {
  
  def start(context: String): Unit
  
  def subjectUri(context: String): Unit
  
  def predicateUri(context: String): Unit
  
  def objectUri(context: String): Unit
  
  def plainLiteral(value: String, isoLang: String): Unit
  
  def typedLiteral(value: String, datatype: String): Unit
  
  def end(context: String): Unit
  
  def result(): String
}

object TripleBuilder{
  // codes for URI positions
  val SUBJECT = 0
  val PREDICATE = 1
  val OBJECT = 2
  val DATATYPE = 3
  val CONTEXT = 4

  // total number of URI positions
  val POSITIONS = 5

  // indicates that a predicate matches all positions
  val ALL = -1
}