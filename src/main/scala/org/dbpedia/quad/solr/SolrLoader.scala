
package org.dbpedia.quad.solr

import java.io.{File, StringReader}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import org.apache.lucene.analysis.{TokenStream, Tokenizer}
import org.apache.lucene.analysis.standard.{StandardTokenizer, StandardTokenizerFactory}
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.dbpedia.quad.file.RichFile
import org.dbpedia.quad.processing._
import org.dbpedia.quad.utils.WikiUtil

import scala.collection.concurrent
import scala.collection.convert.decorateAsJava._
import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration

/**
  * This will upload SOLR documents into a SOLR instance. Input are sorted distributions, to collect star-shaped views of entities.
  * TODO: generify this!
  * Created by chile on 07.06.17.
  */
object SolrLoader {

  private val rdfsLabel = "http://www.w3.org/2000/01/rdf-schema#label"
  private val rdfsComment = "http://www.w3.org/2000/01/rdf-schema#comment"
  private val foafName = "http://xmlns.com/foaf/0.1/name"      //=> altLabel
  private val foafNick = "http://xmlns.com/foaf/0.1/nick"
  private val rdfType = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  private val dctSubject = "http://purl.org/dc/terms/subject"
  private val owlSameAs = "http://www.w3.org/2002/07/owl#sameAs"
  private val dboAltTitle = "http://dbpedia.org/ontology/alternativeTitle"
  private val dboTitle = "http://dbpedia.org/ontology/title"
  private val dboTag = "http://dbpedia.org/ontology/tag"
  private val dboOrigTitle = "http://dbpedia.org/ontology/originalTitle"
  private val dboSubTitle = "http://dbpedia.org/ontology/subtitle"
  private val skosExact = "http://www.w3.org/2004/02/skos/core#exactMatch"
  private val vrank = "http://purl.org/voc/vrank#rankValue"

  private val disambiguations: concurrent.Map[String, ListBuffer[String]] = new ConcurrentHashMap[String, ListBuffer[String]]().asScala
  private val redirects: concurrent.Map[String, ListBuffer[String]] = new ConcurrentHashMap[String, ListBuffer[String]]().asScala
  private val reversedisambiguations: concurrent.Map[String, Int] = new ConcurrentHashMap[String, Int]().asScala
  private val reverseredirects: concurrent.Map[String, Int] = new ConcurrentHashMap[String, Int]().asScala

  //private var finder: DateFinder[File] = _
  private var suffix: String = _
  private var prefix: String = _
  private var solrHandler: SolrHandler = _
  private var nonCommittedDocs: Int = 0
  private var solrCommitPromise: Future[Boolean] = Future[Boolean]{true}

  private val docQueue = new ListBuffer[KgSorlInputDocument]()
  private var lastQueue = new ListBuffer[KgSorlInputDocument]()

  private var baseDir: File = _

  private def redirectWorker = PromisedWork(1.5, 1.0) { language: String =>
    new QuadMapper().readQuads(language, new RichFile(new File(baseDir, prefix + "redirects" + suffix))) { quad =>
      redirects.get(quad.value) match{
        case Some(l) => l.append(quad.subject)
        case None => {
          val zw = new ListBuffer[String]()
          zw.append(quad.subject  )
          redirects.put(quad.value, zw)
        }
      }
      reverseredirects.put(quad.subject, 0)
    }
  }

  private def disambWorker = PromisedWork(1.5, 1.0) { language: String =>
    new QuadMapper().readQuads(language, new RichFile(new File(baseDir, prefix + "disambiguations" + suffix))) { quad =>
      disambiguations.get(quad.value) match{
        case Some(l) => l.append(quad.subject)
        case None => {
          val zw = new ListBuffer[String]()
          zw.append(quad.subject  )
          disambiguations.put(quad.value, zw)
        }
      }
      reversedisambiguations.put(quad.subject, 0)
    }
  }

  /**
    * Async document commit to Solr -> so we don't have to wait for its result and can gather more docs in the meantime
    * @param docs
    * @return
    */
  private def commitDocQueue(docs: List[KgSorlInputDocument]) = Future {
      solrHandler.addSolrDocuments(docs.asJava)
      true
    }

  def main(args: Array[String]): Unit = {

    require(args != null && args.length == 7, "Seven arguments required, extraction config file")


    baseDir = new File(args(0))
    require(baseDir.isDirectory && baseDir.canRead && baseDir.canWrite, "Please specify a valid local base extraction directory - invalid path: " + baseDir)

    val language = args(1)
    prefix = args(2)
    suffix = args(3)
    solrHandler = new SolrHandler(args(4))
    val leadFile = new RichFile(new File(baseDir, prefix + args(5) + suffix))
    val sortedInputs = args(6).split(",").toSeq.map(x => new RichFile(new File(baseDir, prefix + x + suffix)))

    solrHandler.deleteAllDocuments()

    val promised = PromisedWork.workInParallel[String, Boolean](List(disambWorker, redirectWorker), List(language))

    PromisedWork.waitAll(promised.map(_.future))

    new QuadReader(null, 2000, " Documents imported into Solr.").readSortedQuads(language, leadFile, sortedInputs) { quads =>
      val doc = new SolrUriInputDocument(quads.head.subject)
      val title = removeUnwanted(WikiUtil.wikiDecode(doc.getId.substring("http://dbpedia.org/resource/".length)))

      //don't safe files and templates
       if (!title.startsWith("File:") && !title.startsWith("Template:")) {
        //ignore disambiguation and redirect pages
        reversedisambiguations.get(doc.getId) match {
          case Some(x) =>
          case None => reverseredirects.get(doc.getId) match {
            case Some(y) =>
            case None => {
              val altlabels = new ListBuffer[String]()
              val sameass = new ListBuffer[String]()
              val subjects = new ListBuffer[String]()
              val types = new ListBuffer[String]()

              var labelAdded = false
              var commentAdded = false

              for (quad <- quads) {
                quad.predicate match {
                  case `rdfsLabel` => {
                    if (!labelAdded) {
                      doc.addFieldData("label", removeUnwanted(quad.value))
                      labelAdded = true
                    }
                    else
                      altlabels.append(quad.value)
                  }
                  case `rdfsComment` if !commentAdded => {
                    doc.addFieldData("comment", quad.value)
                    commentAdded = true
                  }
                  case `rdfType` => doc.addFieldData("typeUri", List(quad.value).asJava) //TODO add other types?
                  case `dctSubject` => subjects.append(quad.value)
                  case `vrank` => if(doc.getSolrInputDocument.getField("pagerank") == null)
                    doc.addFieldData("pagerank", quad.value.toFloat)
                  case `owlSameAs` | `skosExact` => sameass.append(quad.value)
                  case `foafName` | `foafNick` | `dboSubTitle` | `dboOrigTitle` | `dboTag` | `dboTitle` | `dboAltTitle` => altlabels.append(quad.value)
                  case _ => if (quad.predicate.startsWith("http://dbpedia.org/ontology") && quad.predicate.toLowerCase().contains("name")) altlabels.append(quad.value)
                }
              }

              doc.addFieldData("id", doc.getId)
              doc.addFieldData("title", title)
              var entries : List[String]  = altlabels.distinct.map(removeUnwanted).toList
              doc.addFieldData("altLabel_phrase", entries.map(x => addPayload("altLabel_phrase", x)).asJava)
              doc.addFieldData("altLabel", entries.asJava)
              doc.addFieldData("sameAsUri", sameass.distinct.toList.asJava)
              entries = sameass.distinct.map(x => WikiUtil.wikiDecode(x.substring(x.lastIndexOf("/") + 1))).toList
              doc.addFieldData("sameAsText_phrase", entries.map(x => addPayload("sameAsText_phrase", x)).asJava)
              doc.addFieldData("sameAsText", entries.asJava)
              doc.addFieldData("subjectsUri", subjects.distinct.toList.asJava)
              entries = subjects.distinct.map(x => WikiUtil.wikiDecode(x.substring(x.lastIndexOf("/") + 1))).toList
              doc.addFieldData("subjectsText", entries.asJava)

              redirects.get(doc.getId) match {
                case Some(x) => {
                  entries = x.map(y => WikiUtil.wikiDecode(y.substring("http://dbpedia.org/resource/".length))).toList
                  //doc.addFieldData("redirectsText_phrase", entries.map(x => addPayload("redirectsText_phrase", x)).asJava)
                  doc.addFieldData("redirectsText", entries.asJava)
                  doc.addFieldData("redirectsUri", x.asJava)

                  for(i <- entries.indices)
                    doc.addFieldData("redirectsText_" + i, entries(i))

                }
                case None =>
              }

              disambiguations.get(doc.getId) match {
                case Some(x) => {
                  entries = x.map(y => WikiUtil.wikiDecode(y.substring("http://dbpedia.org/resource/".length))).toList
                  doc.addFieldData("disambiguationsText_phrase", entries.map(x => addPayload("disambiguationsText_phrase", x)).asJava)
                  doc.addFieldData("disambiguationsText", entries.asJava)
                  doc.addFieldData("disambiguationsUri", x.map(y => y).asJava)
                }
                case None =>
              }

              nonCommittedDocs = nonCommittedDocs + 1
              if (nonCommittedDocs % 20000 == 0) {
                docQueue.append(doc)

                Await.result(solrCommitPromise, Duration.apply(2l, TimeUnit.MINUTES))
                //wait if the last commit to solr has not finished yet (quiet unlikely)
                var trys = 0
                while(! solrCommitPromise.value.get.get && trys < 5) {
                  System.out.println("Retrying " + nonCommittedDocs + " documents into Solr.")
                  solrCommitPromise = commitDocQueue(lastQueue.toList).recover{
                    case t: Throwable => {
                      t.printStackTrace()
                      false
                    }
                  }
                  Await.result(solrCommitPromise, Duration.apply(2l, TimeUnit.MINUTES))
                  trys += 1
                }

                System.out.println("Importing " + nonCommittedDocs + " documents into Solr.")
                solrCommitPromise = commitDocQueue(lastQueue.toList).recover{
                  case t: Throwable => {
                    t.printStackTrace()
                    false
                  }
                }
                //clear queue
                lastQueue = docQueue
                docQueue.clear()
              }
              else
                docQueue.append(doc)

            }
          }
        }
      }
    }

    //wait if the last commit to solr has not finished yet (quiet unlikely)
    Await.result(solrCommitPromise, Duration.apply(2l, TimeUnit.MINUTES) )
    //commit current queue
    solrCommitPromise = commitDocQueue(docQueue.toList)
    //wait for the last time
    Await.result(solrCommitPromise, Duration.apply(2l, TimeUnit.MINUTES) )
    System.out.println(nonCommittedDocs + " documents were successfully imported.")
  }

  //TODO make this configurable
  val replacePatterns = Map(
    "^Category:" -> ""
  )

  def removeUnwanted(tag: String) : String ={
    var res = tag
    for(replace <- replacePatterns)
      res = res.replaceAll(replace._1, "")
    res
  }

  private val analyzer = new PayloadAnalyzer()
  def addPayload(field: String, entry: String): String ={
/*    var tokens = new ListBuffer[String]()
    val ts: TokenStream = analyzer.tokenStream(field, new StringReader(entry))
    ts.reset()
    while (ts.incrementToken()){
      tokens += ts.getAttribute(classOf[CharTermAttribute]).toString
    }
    ts.close()
    if(tokens.nonEmpty)
      tokens.map(x => x + "|" + tokens.size).toList.reduceLeft((x,y) => x + " " + y)
    else
      ""*/
    entry
  }
}

