package org.dbpedia.quad.scripts

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import org.dbpedia.quad.Quad
import org.dbpedia.quad.config.Config
import org.dbpedia.quad.destination.{DestinationUtils, FilterDestination, FilterParams}
import org.dbpedia.quad.file.{FileLike, RichFile}
import org.dbpedia.quad.formatters.Formatter
import org.dbpedia.quad.processing.QuadReader
import org.dbpedia.quad.utils.RdfNamespace

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}
/**
  * Created by chile on 14.06.17.
  */
abstract class DumpSplitter(file: FileLike[_]) {

  val destinations: concurrent.Map[String, FilterDestination] = new ConcurrentHashMap[String, FilterDestination]().asScala
  val writersDestinations: concurrent.Map[FilterDestination, Int] = new ConcurrentHashMap[FilterDestination, Int]().asScala

  def start = {
      new QuadReader(null, 2000).readQuads("--", file) { quad =>
        processQuad(quad)
      }
    destinations.values.foreach(_.close())
  }

  def registerNewDestination(fd: FilterDestination): Unit ={
    fd.open()
    fd.params.predicateFilter.foreach(x => destinations.put(x, fd))
    writersDestinations.put(fd, 0)
  }

  def processQuad(quad: Quad) : Unit
}

class PredicateDumpSplitter(baseDir: FileLike[_], file: FileLike[_], predicates: Map[String, Seq[String]], formats: Map[String, Formatter], defaultSet: Boolean)
  extends DumpSplitter(file) {

  DumpSplitter.predicatesToDestination(file, predicates, formats, defaultSet).foreach(x => this.registerNewDestination(x))

  override def processQuad(quad: Quad): Unit = {
    destinations.get(quad.predicate) match{
      case Some(fd) => fd.write(Seq(quad))
      case None if defaultSet => {
        for ((suffix, format) <- formats) {
          val params = FilterParams(null, quad.predicate, null, null, null)
          val iri = quad.predicate.substring(0, Math.max(quad.predicate.lastIndexOf("/"), quad.predicate.lastIndexOf("#")) + 1)
          val ns = RdfNamespace.findPrefix(iri)
          val t = quad.predicate -> DestinationUtils.getDatasetDestination(baseDir, ns.prefix + "-" + quad.predicate.substring(ns.namespace.length), suffix, format, params)
          this.registerNewDestination(t._2)
        }
      }
      case _ =>
    }
  }
}

object DumpSplitter {
  def main(args: Array[String]): Unit ={
    val config = new Config(args(0))

    //TODO get params and predicates
    val predicates = new mutable.HashMap[String, Seq[String]]()
    val inputFile = new RichFile(new File(config.dumpDir.getFile, config.inputDatasets.head + config.inputSuffix))
    val splitter = new PredicateDumpSplitter(config.dumpDir, inputFile, predicates.toMap, config.formats.toMap, true)

    splitter.start
  }

  def predicatesToDestination(baseDir: FileLike[_], predicates: Map[String, Seq[String]], formats: Map[String, Formatter], addDefault: Boolean): Seq[FilterDestination] ={
    val dests = new ListBuffer[FilterDestination]()
    for(pred <- predicates){
      for((suffix, format) <- formats) {
        val params = FilterParams(null, pred._2.foldLeft("")(_ + _), null, null, null)
        dests.append(DestinationUtils.getDatasetDestination(baseDir, pred._1, suffix, format, params))
      }
    }
    dests.toList
  }
}