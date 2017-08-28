package org.dbpedia.quad.processing

import java.lang.StringBuilder
import java.io.File

import org.dbpedia.quad.Quad
import org.dbpedia.quad.destination.{Destination, WriterDestination}
import org.dbpedia.quad.file.FileLike
import org.dbpedia.quad.formatters.TerseFormatter
import org.dbpedia.quad.file.IOUtils.writer
import org.dbpedia.quad.log.LogRecorder

import scala.Console.err

/**
 * Maps old quads/triples to new quads/triples.
 */
class QuadMapper(rec: LogRecorder[Quad]) extends QuadReader(rec) {

  def this(log: FileLike[File] = null, reportInterval: Int = 100000, preamble: String = null) = this(LogRecorder.create[Quad](log, reportInterval, preamble))

  /**
    * @deprecated don't use it any more!
    */
  @Deprecated
  private def quadToString(quad: Quad): String = {
    val sb = new StringBuilder
    sb append '<' append quad.subject append "> <" append quad.predicate append "> "
    if (quad.datatype == null) {
      sb append '<' append quad.value append "> "
    }
    else {
      sb append '"' append quad.value append '"'
      if (quad.datatype != "http://www.w3.org/2001/XMLSchema#string") sb append "^^<" append quad.datatype append "> "
      else if (quad.language != null) sb append '@' append quad.language append ' '
    }
    if (quad.context != null) sb append '<' append quad.context append "> "
    sb append ".\n"
    sb.toString
  }

  /**
   */
  def mapQuads(language: String, inFile: FileLike[_], outFile: FileLike[_], required: Boolean, quads: Boolean, turtle: Boolean)(map: Quad => Traversable[Quad]): Boolean = {
    err.println(language+": writing "+outFile+" ...")
    val destination = new WriterDestination(() => writer(outFile), new TerseFormatter(quads, turtle))
    mapQuads(language, inFile, destination, required, true)(map)
  }

  /**
   * TODO: do we really want to open and close the destination here? Users may want to map quads
   * from multiple input files to one destination. On the other hand, if the input file doesn't
   * exist, we probably shouldn't open the destination at all, so it's ok that it's happening in
   * this method after checking the input file.
    * Chile: made closing optional, also WriterDestination can only open Writer once now
   */
  def mapQuads(language: String, inFile: FileLike[_], destination: Destination, required: Boolean, closeWriter: Boolean)(map: Quad => Traversable[Quad]): Boolean = {
    
    if (! inFile.exists) {
      if (required) throw new IllegalArgumentException(language+": file "+inFile+" does not exist")
      return false      //return stating that the file was not completely read
    }

    destination.open()
    val ret = try {
      readQuads(language, inFile) { old =>
        destination.write(map(old))
      }
    }
    finally
      if(closeWriter)
        destination.close()

    ret
  }

  /**
    * TODO: do we really want to open and close the destination here? Users may want to map quads
    * from multiple input files to one destination. On the other hand, if the input file doesn't
    * exist, we probably shouldn't open the destination at all, so it's ok that it's happening in
    * this method after checking the input file.
    */
  def mapSortedQuads(language: String, inFile: FileLike[_], destination: Destination, required: Boolean)(map: Traversable[Quad] => Traversable[Quad]): Boolean = {

    if (! inFile.exists) {
      if (required) throw new IllegalArgumentException(language+": file "+inFile+" does not exist")
      err.println(language+": WARNING - file "+inFile+" does not exist")
      return false
    }

    var mapCount = 0
    destination.open()
    val ret = try {
      readSortedQuads(language, inFile) { old =>
        destination.write(map(old))
        mapCount += old.size
      }
    }
    finally destination.close()
    err.println(language+": mapped "+mapCount+" quads")

    ret
  }
}

object QuadMapper{
  private def createWithRecorder(rec: LogRecorder[Quad]): QuadReader = new QuadReader(rec)
}