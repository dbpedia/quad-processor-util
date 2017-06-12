package org.dbpedia.quad.processing

import java.io.File
import java.util.Comparator

import org.dbpedia.quad.Quad
import org.dbpedia.quad.file.{BufferedLineReader, FileLike, IOUtils}
import org.dbpedia.quad.log.{LogRecorder, RecordEntry, RecordSeverity}
import org.dbpedia.quad.utils.StringUtils

import scala.Console.err
import scala.collection.mutable.ListBuffer
import org.dbpedia.quad.processing.PromisedWork._

import scala.util.Try

/**
 */
class QuadReader(log: FileLike[File] = null, reportInterval: Int = 100000, preamble: String = null) {

  def isSpecialChar(char: Int) = char == 32 || char <= 47 && char >= 34 || char <= 63 && char >= 58 || char <= 96 && char >= 91 || char <= 126 && char >= 123;

  private val ucsb = new StringBuilder()

  var lastChar = " "
  for(i <- 1 to 40000){
    var char = i.asInstanceOf[Char]
    if(i != 40860 && !lastChar.equals(char.toString) && !char.isControl && !char.isIdentifierIgnorable && !char.isSpaceChar) {
      if (isSpecialChar(i))
        ucsb.append("< '" + char + "' ")
      else
        ucsb.append("< " + char + " ")

    }
    lastChar = char.toString
  }

  private val recorder: LogRecorder[Quad] = Option(log) match{
    case Some(f) => new LogRecorder[Quad](IOUtils.writer(f, append = true), reportInterval, preamble)
    case None => new LogRecorder[Quad](null, reportInterval, preamble)
  }

  def getRecorder = recorder

  def addQuadRecord(quad: Quad, lang: String, errorMsg: String = null, error: Throwable = null): Unit ={
    if(errorMsg == null && error == null)
      recorder.record(new RecordEntry[Quad]("", "", quad, RecordSeverity.Info, lang, errorMsg, error))
    else if(error != null)
      recorder.record(new RecordEntry[Quad]("", "", quad, RecordSeverity.Exception, lang, errorMsg, error))
    else
      recorder.record(new RecordEntry[Quad]("", "", quad, RecordSeverity.Warning, lang, errorMsg, error))
  }

  val comparator = new Comparator[String] {
    override def compare(t: String, t1: String): Int = {
      for(i <- 0 until Math.min(t.length, t1.length)){
        val char1 = Character.codePointAt(t, i)
        val char2 = Character.codePointAt(t1, i)
        if(char1 < char2)
          return -1
        if(char1 > char2)
          return 1
      }
      if(t1.length > t.length)
        return -1
      if(t.length > t1.length)
        return 1
      0
    }
  }

  def this(){
    this(null, 100000, null)
  }


  private def getReaderWorker(reader: BufferedLineReader) =
    SimpleWorkers[String, Seq[Quad]](1.5, 1.0) { param: String =>
    //peek to get the current quad (and not the next as with readToQuad())
    val buffer = new ListBuffer[Quad]
    var readerQuad: Quad = readToQuad(reader)
    while (readerQuad != null && comparator.compare(readerQuad.subject, param) < 0){
      readerQuad = readToQuad(reader, true)
    }
    while (readerQuad != null && comparator.compare(readerQuad.subject, param) == 0){
      buffer.append(readerQuad)
      var testQuad = Quad.unapply(reader.peek) match{
        case Some(q) => q
        case None => readToQuad(reader, true)
      }
      while(testQuad.subject == readerQuad.subject && testQuad.value == readerQuad.value)
        testQuad = readToQuad(reader, true)
      readerQuad = testQuad
    }
    buffer.toList
  }

  def readSortedQuads[T <% FileLike[T]](language: String, file: FileLike[_])(proc: Traversable[Quad] => Unit): Unit = {
    //TODO needs extraction-recorder syntax!
    var lastSubj = ""
    var seq = ListBuffer[Quad]()
    readQuads(language, file) { quad =>
      if(!lastSubj.equals(quad.subject))
      {
        lastSubj = quad.subject
        if(seq.nonEmpty)
          proc(seq.toList)
        seq.clear()
        seq += quad
      }
      else{
        seq += quad
      }
    }
    if(seq.nonEmpty)
      proc(seq.toList)
  }

  def readSortedQuads[T <% FileLike[T]](language:String, leadFile: FileLike[_], files: Seq[FileLike[_]])(proc: Traversable[Quad] => Unit): Unit = {

    val readers = files.map(IOUtils.bufferedReader(_))
    val workers = readers.map(x => getReaderWorker(x))

    readSortedQuads[T](language, leadFile){ quads =>
      if(quads.isEmpty)
        return
      val subj = quads.head.subject

      val futureQuads = for (worker <- workers)
        yield worker.work(subj)

      waitAll(futureQuads.map(x => x.future))
      val zw = futureQuads.map(x => x.future.value).map(y => y.getOrElse(Try{Seq()}).getOrElse(Seq())).flatten  //TODO make this more readable and insert recovery!
      proc(zw ++ quads)
    }
    readers.foreach(_.close())
  }

  private def readToQuad(reader: BufferedLineReader, forceNewLine: Boolean = false): Quad = {
    var readerQuad : Quad = Quad.unapply(if(forceNewLine) reader.readLine() else reader.peek) match{
      case Some(q) => q
      case None => null
    }
    while (reader.hasMoreLines && readerQuad == null)
      Quad.unapply(reader.peek) match {
        case Some(q) => readerQuad = q
        case None => reader.readLine()
      }
    readerQuad
  }


  /**
   * @param language for logging
   * @param file input file
   * @param proc process quad
   */
  def readQuads(language: String, file: FileLike[_])(proc: Quad => Unit): Unit = {
    val dataset = "(?<=(.*wiki-\\d{8}-))([^\\.]+)".r.findFirstIn(file.toString) match {
      case Some(x) => x
      case None => null
    }
    getRecorder.initialize(language)

    IOUtils.readLines(file) { line =>
      line match {
        case null => // ignore last value
        case Quad(quad) => {
          val copy = quad.copy (
            dataset = dataset
          )
          proc(copy)
          addQuadRecord(copy, language)
        }
        case str => if (str.nonEmpty && !str.startsWith("#"))
          addQuadRecord(null, language, null, new IllegalArgumentException("line did not match quad or triple syntax: " + line))
      }
    }
    addQuadRecord(null, language, "reading quads completed with {page} pages", null)
  }
  
  private def logRead(tag: String, lines: Int, start: Long): Unit = {
    val micros = (System.nanoTime - start) / 1000
    err.println(tag+": read "+lines+" lines in "+ StringUtils.prettyMillis(micros / 1000)+" ("+(micros.toFloat / lines)+" micros per line)")
  }
}