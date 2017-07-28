package org.dbpedia.quad.config

import java.io.File
import java.net.{URI, URL}
import java.util.Properties
import java.util.logging.{Level, Logger}

import org.dbpedia.quad.config.ConfigUtils._
import org.dbpedia.quad.file.RichFile
import org.dbpedia.quad.formatters.{Formatter, RDFJSONFormatter, TerseFormatter, TriXFormatter}

import scala.collection.Map
import scala.collection.mutable.HashMap
import scala.util.{Failure, Success}

import scala.collection.convert.decorateAsScala._

class Config(val configPath: String) extends
  Properties(Config.universalProperties)
{

  if(configPath != null)
    this.putAll(ConfigUtils.loadConfig(configPath))

  val logger = Logger.getLogger(getClass.getName)
  /**
    * load two config files:
    * 1. the universal config containing properties universal for a release
    * 2. the extraction job specific config provided by the user
    */

  def getArbitraryStringProperty(key: String): Option[String] = {
    Option(getString(this, key))
  }

  def throwMissingPropertyException(property: String, required: Boolean): Unit ={
    if(required)
      throw new IllegalArgumentException("The following required property is missing from the provided .properties file (or has an invalid format): '" + property + "'")
    else
      logger.log(Level.WARNING, "The following property is missing from the provided .properties file (or has an invalid format): '" + property + "'. It will not factor in.")
  }

  /**
    * get all universal properties, check if there is an override in the provided config file
    */


  // TODO Watch out, this could be a regex
  lazy val source: String = this.getProperty("source", "pages-articles.xml.bz2").trim

  lazy val wikiName: String = this.getProperty("wiki-name", "wiki").trim

  /**
   * Dump directory
   * Note: This is lazy to defer initialization until actually called (eg. this class is not used
   * directly in the distributed extraction framework - DistConfig.ExtractionConfig extends Config
   * and overrides this val to null because it is not needed)
   */
  lazy val dumpDir: RichFile = getValue(this, "base-dir", required = true){ x => new File(x)}

  lazy val parallelProcesses: Int = this.getProperty("parallel-processes", "4").trim.toInt

  lazy val dbPediaVersion: String = parseVersionString(getString(this, "dbpedia-version").trim) match{
    case Success(s) => s
    case Failure(e) => throw new IllegalArgumentException("dbpedia-version option in universal.properties was not defined or in a wrong format", e)
  }

  lazy val wikidataMappingsFile: RichFile = {
    val name = this.getProperty("wikidata-property-mappings-file", "wikidata-property-mappings.json").trim
    new RichFile(new File(dumpDir.getFile, name))
  }

  /**
    * The directory where all log files will be stored
    */
  lazy val logDir: Option[File] = Option(getString(this, "log-dir")) match {
    case Some(x) => Some(new File(x))
    case None => None
  }


  /**
    * Local ontology file, downloaded for speed and reproducibility
    * Note: This is lazy to defer initialization until actually called (eg. this class is not used
    * directly in the distributed extraction framework - DistConfig.ExtractionConfig extends Config
    * and overrides this val to null because it is not needed)
    */
  lazy val ontologyFile: File = getValue(this, "ontology", required = false)(new File(_))

  lazy val prefixMap : Map[String, String] = getStringMap(this, "prefix-map", ";")

  /**
    * all non universal properties...
    */

  lazy val formats: Map[String, Formatter] = parseFormats(this, "format")
  /**
    * An array of input dataset names (e.g. 'instance-types' or 'mappingbased-literals') (separated by a ',')
    */
  lazy val inputDatasets: Seq[String] = getStrings(this, "input-datasets", ",").distinct   //unique collection of names!!!
  /**
    * A dataset name for the output file generated (e.g. 'instance-types' or 'mappingbased-literals')
    */
  lazy val outputDataset: Option[String] = Option(getString(this, "output"))
  /**
    * the suffix of the files representing the input dataset (usually a combination of RDF serialization extension and compression used - e.g. .ttl.bz2 when using the TURTLE triples compressed with bzip2)
    */
  lazy val inputSuffix: String = getString(this, "suffix")
  /**
    * same as for inputSuffix (for the output dataset)
    */
  lazy val outputSuffix: Option[String] = Option(getString(this, "output-suffix"))
  /**
    * instead of a defined output dataset name, one can specify a name extension turncated at the end of the input dataset name (e.g. '-transitive' -> instance-types-transitive)
    */
  lazy val datasetnameExtension: Option[String] = Option(getString(this, "name-extension"))

  /**
    * An array of languages specified by the exact enumeration of language wiki codes (e.g. en,de,fr...)
    * or article count ranges ('10000-20000' or '10000-' -> all wiki languages having that much articles...)
    * or '@mappings', '@chapters' when only mapping/chapter languages are of concern
    * or '@downloaded' if all downloaded languages are to be processed (containing the download.complete file)
    * or '@abstracts' to only process languages which provide human readable abstracts (thus not 'wikidata' and the like...)
    */
  lazy val languages: Seq[String] = getStrings(this, "languages", ",")

  /**
    * before processing a given language, check if the download.complete file is present
    */
  lazy val requireComplete: Boolean = this.getProperty("require-download-complete", "false").trim.toBoolean

  /**
    * TODO experimental, ignore for now
    */
  lazy val retryFailedPages: Boolean = this.getProperty("retry-failed-pages", "false").trim.toBoolean


  private val formatters = Map[String, Formatter] (
    "trix-triples" -> { new TriXFormatter(false) },
    "trix-quads" -> { new TriXFormatter(true) },
    "turtle-triples" -> { new TerseFormatter(false, true) },
    "turtle-quads" -> { new TerseFormatter(true, true) },
    "n-triples" -> { new TerseFormatter(false, false) },
    "n-quads" -> { new TerseFormatter(true, false) },
    "rdf-json" -> { new RDFJSONFormatter() }
  )

  def getFormatter: Option[Formatter] = formats.get(outputSuffix match{
    case Some(x) => if(x.startsWith(".")) x.substring(1) else x
    case None => if(inputSuffix.startsWith(".")) inputSuffix.substring(1) else inputSuffix
  })

  /**
    * Parse all format lines.
    * @param prefix format key prefix, e.g. "format"
    * @return map from file suffix (without '.' dot) to formatter
    */
  def parseFormats(config: Properties, prefix: String): Map[String, Formatter] = {

    val dottedPrefix = prefix + "."
    val formats = new HashMap[String, Formatter]()

    for (key: String <- config.stringPropertyNames().asScala.toList) {

      if (key.startsWith(dottedPrefix)) {

        val suffix = key.substring(dottedPrefix.length)

        val settings = getStrings(config, key, ";", true)
        require(settings.length == 1 || settings.length == 2, "key '"+key+"' must have one or two values separated by ';' - file format and optional uri policy name")

        val formatter =
          formatters.getOrElse(settings.head, throw error("first value for key '"+key+"' is '"+settings(0)+"' but must be one of "+formatters.keys.toSeq.sorted.mkString("'","','","'")))

        formats(suffix) = formatter
      }
    }
    formats
  }
}

object Config{

  case class NifParameters(
    nifQuery: String,
    nifTags: String,
    isTestRun: Boolean,
    writeAnchor: Boolean,
    writeLinkAnchor: Boolean,
    abstractsOnly: Boolean,
    cssSelectorMap: URL
  )

  case class MediaWikiConnection(
    apiUrl: String,
    maxRetries: Int,
    connectMs: Int,
    readMs: Int,
    sleepFactor: Int
  )

  case class AbstractParameters(
    abstractQuery: String,
    shortAbstractsProperty: String,
    longAbstractsProperty: String,
    shortAbstractMinLength: Int,
    abstractTags: String
  )

  case class SlackCredentials(
     webhook: URL,
     username: String,
     summaryThreshold: Int,
     exceptionThreshold: Int
   )

  private val universalProperties: Properties = loadConfig(this.getClass.getClassLoader.getResource("universal.properties")).asInstanceOf[Properties]
  val UniversalConfig: Config = new Config(null)

}
