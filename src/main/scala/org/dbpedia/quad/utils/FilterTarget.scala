package org.dbpedia.quad.utils

import org.dbpedia.quad.Quad

/**
  * Created by chile on 14.06.17.
  */

object FilterTarget extends Enumeration {
  val
  subject,
  predicate,
  value,
  graph = Value

  def resolveQuadResource(quad: Quad, target: FilterTarget.Value): String = target match{
    case FilterTarget.graph => quad.context
    case FilterTarget.predicate => quad.predicate
    case FilterTarget.subject => quad.subject
    case FilterTarget.value => quad.value
  }
}
