package org.dbpedia.quad.formatters

import org.dbpedia.quad.Quad

/**
 * Serializes statements.
 */
trait Formatter
{
  def setHeader(head: String): Unit

  def header: String
  
  def footer: String

  def setFooter(foot: String): Unit

  def render(quad: Quad): String

  def serialization: String
}
