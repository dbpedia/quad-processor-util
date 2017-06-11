package org.dbpedia.quad.formatters

import java.net.{IDN, URI, URISyntaxException}
import java.util.Properties

import scala.collection.JavaConversions.asScalaSet
import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashMap}
import java.util.Properties
import java.net.{IDN, URI, URISyntaxException}

import org.dbpedia.quad.utils.RichString

import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.collection.JavaConversions.asScalaSet
/**
 * TODO: use scala.collection.Map[String, String] instead of java.util.Properties?
 */
object UriPolicy {

  /**
   * A policy is a function takes a URI and may return the given URI or a transformed version of it.
   * Human-readable type alias.
   */
  type Policy = URI => URI

  /**
   * PolicyApplicable is a function that decides if a policy should be applied for the given DBpedia URI.
   * Human-readable type alias.
   */
  type PolicyApplicable = URI => Boolean

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

  /*
  Methods to parse the lines for 'uri-policy' in extraction configuration files. These lines
  determine how extracted triples are formatted when they are written to files. For details see
  https://github.com/dbpedia/extraction-framework/wiki/Serialization-format-properties
  */

  /**
   * Key is full policy name, value is triple of priority, position code and factory.
   */
  private val policies: Map[String, (Int, Int, PolicyApplicable => Policy)] = locally {

    /**
     * Triples of prefix, priority and factory.
     *
     * Priority is important:
     *
     * 1. check length
     * 2. convert IRI to URI
     * 3. append '_' if necessary
     * 4. convert specific domain to generic domain.
     *
     * The length check must happen before the URI conversion, because for a non-Latin IRI the URI
     * may be several times as long, e.g. one Chinese character has several UTF-8 bytes, each of
     * which needs three characters after percent-encoding.
     *
     * The third step must happen after URI conversion (because a URI may need an underscore where
     * a IRI doesn't), and before the last step (because we need the specific domain to decide which 
     * URIs should be made xml-safe).
     */
    val policies = Seq[(String, Int, PolicyApplicable => Policy)] (
      ("reject-long", 1, rejectLong),
      ("uri", 2, uri),
      ("xml-safe", 3, xmlSafe),
      ("generic", 4, generic)
    )

    /**
     * Tuples of suffix and position code.
     */
    val positions = Seq[(String, Int)] (
      ("", ALL),
      ("-subjects", SUBJECT),
      ("-predicates", PREDICATE),
      ("-objects", OBJECT),
      ("-datatypes", DATATYPE),
      ("-contexts", CONTEXT)
    )

    val product = for ((prefix, prio, factory) <- policies; (suffix, position) <- positions) yield {
      prefix+suffix -> (prio, position, factory)
    }

    product.toMap
  }

  private val formatters = Map[String, Array[Policy] => Formatter] (
    "trix-triples" -> { new TriXFormatter(false, _) },
    "trix-quads" -> { new TriXFormatter(true, _) },
    "turtle-triples" -> { new TerseFormatter(false, true, _) },
    "turtle-quads" -> { new TerseFormatter(true, true, _) },
    "n-triples" -> { new TerseFormatter(false, false, _) },
    "n-quads" -> { new TerseFormatter(true, false, _) },
    "rdf-json" -> { new RDFJSONFormatter(_) }
  )

  private def error(message: String, cause: Throwable = null): IllegalArgumentException = {
    new IllegalArgumentException(message, cause)
  }

  /*
  Methods that check URIs at run-time.
  */

  def uri(applicableTo: PolicyApplicable): Policy = {

    iri =>
      if (applicableTo(iri)) {
        toUri(iri)
      }
      else {
        iri
      }
  }

  def toUri(iri: URI) : URI = {
    // In IDN URI parses the host as Authoriry
    // see https://github.com/dbpedia/extraction-framework/pull/300#issuecomment-67777966
    if (iri.getHost() == null && iri.getAuthority != null ) {
      try {
        var host: String = iri.getAuthority
        var user: String = null
        var port = -1

        val userIndex = host.indexOf('@')
        if (userIndex >= 0) {
          user = host.substring(0, userIndex)
          host = host.replace(user + "@", "")
        }

        val portIndex = host.indexOf(':')
        if (portIndex >= 0) {
          port = Integer.parseInt(host.substring(portIndex+1))
          host = host.replace(":" + port, "")
        }
        host = IDN.toASCII(host)

        new URI(uri(iri.getScheme, user, host, port, iri.getPath, iri.getQuery, iri.getFragment).toASCIIString)
      } catch {
        case _: NumberFormatException | _: IllegalArgumentException => new URI(iri.toASCIIString)
      }

    } else {
      new URI(iri.toASCIIString)
    }
  }

  def generic(applicableTo: PolicyApplicable): Policy = {

    iri =>
      if (applicableTo(iri)) {

        val scheme = iri.getScheme
        val user = iri.getRawUserInfo
        val host = "dbpedia.org"
        val port = iri.getPort
        val path = iri.getRawPath
        val query = iri.getRawQuery
        val frag = iri.getRawFragment

        uri(scheme, user, host, port, path, query, frag)
      }
      else {
        iri
      }
  }

  // max length (arbitrary choice)
  val MAX_LENGTH = 500

  def rejectLong(applicableTo: PolicyApplicable): Policy = {

    iri =>
      if (applicableTo(iri)) {
        val str = iri.toString
        if (str.length > MAX_LENGTH) throw new URISyntaxException(str, "length "+str.length+" exceeds maximum "+MAX_LENGTH)
      }
      iri
  }

  /**
   * Check if the tail of the URI could be used as an XML element name. If not, attach an
   * underscore (which is a valid XML name). The resulting URI is guaranteed to be usable
   * as a predicate in RDF/XML - it can be split into a valid namespace URI and a valid XML name.
   *
   * Examples:
   *
   * original URI       xml safe URI (may be equal)   possible namespace and name in RDF/XML
   *
   * http://foo/bar          http://foo/bar           http://foo/           bar
   * http://foo/123          http://foo/123_          http://foo/123        _
   * http://foo/%22          http://foo/%22_          http://foo/%22        _
   * http://foo/%C3%BC       http://foo/%C3%BC_       http://foo/%C3%BC     _
   * http://foo/%C3%BCD      http://foo/%C3%BCD       http://foo/%C3%BC     D
   * http://foo/%            http://foo/%22_          http://foo/%22        _
   * http://foo/bar_(fub)    http://foo/bar_(fub)_    http://foo/bar_(fub)  _
   * http://foo/bar#a123     http://foo/bar#a123      http://foo/bar#       a123
   * http://foo/bar#123      http://foo/bar#123_      http://foo/bar#123    _
   * http://foo/bar#         http://foo/bar#_         http://foo/bar#       _
   * http://foo/bar?a123     http://foo/bar?a123      http://foo/bar?       a123
   * http://foo/bar?a=       http://foo/bar?a=_       http://foo/bar?a=     _
   * http://foo/bar?a=b      http://foo/bar?a=b       http://foo/bar?a=     b
   * http://foo/bar?123      http://foo/bar?123_      http://foo/bar?123    _
   * http://foo/bar?         http://foo/bar?_         http://foo/bar?       _
   * http://foo/             http://foo/_             http://foo/           _
   * http://foo              http://foo/_             http://foo/           _
   * http://foo/:            http://foo/:_            http://foo/:          _
   * http://foo/a:           http://foo/a:_           http://foo/a:         _
   * http://foo/a:b          http://foo/a:b           http://foo/a:         b
   */
  def xmlSafe(applicableTo: PolicyApplicable): Policy = {

    iri =>
      if (applicableTo(iri)) {

        val scheme = iri.getScheme
        val user = iri.getRawUserInfo

        // When the URI is IDN the URI parser puts the domain name in Authority
        val host = if (iri.getHost == null && iri.getAuthority != null) iri.getAuthority else iri.getHost
        val port = iri.getPort
        var path = iri.getRawPath
        var query = iri.getRawQuery
        var frag = iri.getRawFragment

        if (frag != null) frag = xmlSafe(frag)
        else if (query != null) query = xmlSafe(query)
        else if (path != null && path.nonEmpty) path = xmlSafe(path)
        else path = "/_" // convert empty path to "/_"

        uri(scheme, user, host, port, path, query, frag)
      }
      else {
        iri
      }
  }

  /**
   * Check if the tail of the string could be used as an XML element name. 
   * If not, attach an underscore (which is a valid XML name).
   */
  private def xmlSafe(tail: String): String = {

    // Go through tail from back to front, find minimal safe part.
    var index = tail.length
    while (index > 0) {

      index -= 1

      // If char is part of a %XX sequence, we can't split the URI into a namespace and a name.
      // Note: We know it's a valid IRI. Otherwise we'd need more checks here. 
      if (index >= 2 && tail.charAt(index - 2) == '%') return tail+'_'

      val ch = tail.charAt(index)

      // If char is not valid as part of a name, we can't use the tail as a name.
      // Note: isNameChar allows ':', but we're stricter.
      if (ch == ':' || ! isNameChar(ch)) return tail+'_'

      // If char is valid as start of a name, we can use this part as a name.
      if (isNameStart(ch)) return tail
    }

    // We can't use the string as an XML name.
    return tail+'_'
  }

  private def uri(scheme: String, user: String, host: String, port: Int, path: String, query: String, frag: String): URI = {

    val sb = new StringBuilder

    if (scheme != null) sb append scheme append ':'

    if (host != null) {
      sb.append("//");
      if (user != null) sb.append(user).append('@');
      sb.append(host);
      if (port != -1) sb.append(':').append(port);
    }

    if (path != null) sb.append(path);
    if (query != null) sb.append('?').append(query);
    if (frag != null) sb.append('#').append(frag);

    new URI(sb.toString)
  }

  /**
    * {{{
    *  NameChar ::= Letter | Digit | '.' | '-' | '_' | ':'
    *             | CombiningChar | Extender
    *  }}}
    *  See [4] and Appendix B of XML 1.0 specification.
    */
  def isNameChar(ch: Char) = {
    import java.lang.Character._
    // The constants represent groups Mc, Me, Mn, Lm, and Nd.

    isNameStart(ch) || (getType(ch).toByte match {
      case COMBINING_SPACING_MARK |
           ENCLOSING_MARK | NON_SPACING_MARK |
           MODIFIER_LETTER | DECIMAL_DIGIT_NUMBER => true
      case _ => ".-:" contains ch
    })
  }


  /**
    * {{{
    *  NameStart ::= ( Letter | '_' )
    *  }}}
    *  where Letter means in one of the Unicode general
    *  categories `{ Ll, Lu, Lo, Lt, Nl }`.
    *
    *  We do not allow a name to start with `:`.
    *  See [3] and Appendix B of XML 1.0 specification
    */
  def isNameStart(ch: Char) = {
    import java.lang.Character._

    getType(ch).toByte match {
      case LOWERCASE_LETTER |
           UPPERCASE_LETTER | OTHER_LETTER |
           TITLECASE_LETTER | LETTER_NUMBER => true
      case _ => ch == '_'
    }
  }
}

