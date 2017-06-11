package org.dbpedia.quad.formatters

import org.dbpedia.quad.Quad

/**
 * Serializes statements.
 */
trait Formatter
{
  def header: String
  
  def footer: String
  
  def render(quad: Quad): String
}
