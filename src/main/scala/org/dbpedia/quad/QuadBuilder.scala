package org.dbpedia.quad

/**
 * Convenience methods that help to unclutter code. 
 */
object QuadBuilder {
  def dynamicPredicate(language: String, dataset: String) (subject: String, predicate: String, value: String, context: String, datatype: String) =
    new Quad(language, dataset, subject, predicate, value, context, datatype)
}
