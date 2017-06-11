package org.dbpedia.quad.formatters

import org.dbpedia.quad.formatters.UriPolicy._
import org.dbpedia.quad.utils.StringUtils

/**
 * TODO: add functionality - the comments could contain more useful info
 * 
 * @param policies Mapping from URI positions (as defined in UriPolicy) to URI policy functions.
 * Must have five (UriPolicy.POSITIONS) elements. If null, URIs will not be modified.
 */
class TerseFormatter(val quads: Boolean, val turtle: Boolean, val policies: Array[Policy] = null)
extends TripleFormatter(() => new TerseBuilder(quads, turtle, policies))
{
  override def header = "# started "+StringUtils.formatCurrentTimestamp+"\n"
  
  override def footer = "# completed "+StringUtils.formatCurrentTimestamp+"\n"
}
