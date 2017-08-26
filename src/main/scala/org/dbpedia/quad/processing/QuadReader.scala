package org.dbpedia.quad.processing

import java.io.File
import java.util.FormatterClosedException

import org.dbpedia.quad.Quad
import org.dbpedia.quad.file.{BufferedLineReader, FileLike, IOUtils, NoMoreLinesException}
import org.dbpedia.quad.log.{LogRecorder, RecordEntry, RecordSeverity}
import org.dbpedia.quad.utils.{FilterTarget, StringUtils}

import scala.Console.err
import scala.collection.mutable.ListBuffer
import org.dbpedia.quad.processing.PromisedWork._
import org.dbpedia.quad.sort.{CodePointComparator, QuadComparator}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.languageFeature.implicitConversions
import scala.concurrent.ExecutionContext.Implicits.global

/**
 */
class QuadReader(log: FileLike[File] = null, reportInterval: Int = 100000, preamble: String = null) {

  private var reader: BufferedLineReader = _
  private var file: FileLike[_] = _

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

  val comparator = new CodePointComparator()

  def this(){
    this(null, 100000, null)
  }


  private def getSubjectGroupReader(reader: BufferedLineReader) =
    PromisedWork[String, Seq[Quad]](1.5, 1.0) { param: String =>
    val buffer = new ListBuffer[Quad]()

      //peek to get the current quad (and not the next as with readToQuad())
      var readerQuad: Option[Quad] = readToQuad(reader)
      //by adding '>' we make sure that the codepoint comparator has the same environment as in the file (the leading '<' is everywhere the same...)
    while (readerQuad.isDefined && comparator.compare( readerQuad.get.subject + ">",   param + ">") < 0){
      readerQuad = readToQuad(reader)
    }
    while (readerQuad.isDefined && comparator.compare(readerQuad.get.subject + ">", param + ">") == 0){
      buffer.append(readerQuad.get)
      readerQuad = readToQuad(reader)
    }
    //set back one line, else we will jump over one
    reader.setBackOneLine()
    buffer.toList
  }

  private def getGroupReader(reader: BufferedLineReader, target: FilterTarget.Value): PromiseIterator[Seq[Quad]] = {
    def quadValue = target match {
      case FilterTarget.subject => (q: Quad) => q.subject
      case FilterTarget.value => (q: Quad) => q.value
      case FilterTarget.predicate => (q: Quad) => q.predicate
      case FilterTarget.graph => (q: Quad) => q.context
    }

    PromiseIterator.byFuture[Seq[Quad]](12, 8) { v: Unit =>
      Future {
        val thisReader = reader
        val buffer = new ListBuffer[Quad]()

        if (thisReader.hasMoreLines) {
          var readerQuad: Option[Quad] = readToQuad(thisReader)
          readerQuad match {
            case Some(x) => {
              val value = quadValue(x)

              //by adding '>' we make sure that the codepoint comparator has the same environment as in the file (the leading '<' is everywhere the same...)

              while (readerQuad.isDefined && comparator.compare(quadValue(readerQuad.get), value) < 0) {
                readerQuad = readToQuad(thisReader)
              }
              while (readerQuad.isDefined && comparator.compare(quadValue(readerQuad.get), value) == 0) {
                buffer.append(readerQuad.get)
                readerQuad = readToQuad(thisReader)
              }
              //set back one line, else we will jump over one
              thisReader.setBackOneLine()
            }
            case None =>
          }
          buffer.toList
        }
        else
          Seq()
      }
    }
  }

  def readSortedQuads[T <% FileLike[T]](tag: String, file: FileLike[_])(proc: Traversable[Quad] => Unit): Boolean = {
    //TODO needs extraction-recorder syntax!
    var lastSubj = ""
    var seq = ListBuffer[Quad]()
    val ret = readQuads(tag, file) { quad =>
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
    ret
  }

  def readSortedQuads[T <% FileLike[T]](tag:String, leadFile: FileLike[_], files: Seq[FileLike[_]])(proc: Traversable[Quad] => Unit): Boolean = {

    val readers = files.map(IOUtils.bufferedReader(_))
    val workers = readers.map(x => getSubjectGroupReader(x))

    val ret = readSortedQuads[T](tag, leadFile){ quads =>
      val subj = quads.head.subject

      val futureQuads = for (worker <- workers)
        yield worker.work(subj)

      waitAll(futureQuads)
      val zw = futureQuads.map(x => x.future.value).map(y => y.getOrElse(Try{Seq()}).getOrElse(Seq())).flatten  //TODO make this more readable and insert recovery!
      proc(zw ++ quads)
    }
    readers.foreach(_.close())
    ret
  }

  def readSortedQuads (tag:String, files: Seq[FileLike[_]], target: FilterTarget.Value = FilterTarget.subject)(proc: Traversable[Quad] => Unit): Boolean = {
    val readers = files.map(x => IOUtils.bufferedReader(x))
    val workers = readers.map(x => getGroupReader(x, target)).toList
    val comp = new QuadComparator(target)

    var treeMap :List[(Promise[Seq[Quad]], PromiseIterator[Seq[Quad]])] = List()
    for (reader <- workers)
        treeMap = treeMap ::: List((reader.next(), reader))

    PromisedWork.waitAll(treeMap.map(x => x._1.future))
    treeMap = treeMap.sortWith((x,y) => comp.compare(x._1.future.value.get.get.head, y._1.future.value.get.get.head) < 0)

    while (true) {
      val head = treeMap.head
      val next = if(head._2.hasNext) (head._2.next(), head._2) else {
        workers.collect { case x if x.hasNext => x }.headOption match {
          case Some(y) => (y.next(), y)
          case None =>
            proc(head._1.future.value.get.get)
            return true
        }
      }
      PromisedWork.waitAll(List(next._1.future))
      val headv = head._1.future.value.get match{
        case Success(s) => if(s.nonEmpty)
          s.head
        else
          throw new Exception("why?")
        case Failure(f) => throw f
      }
      val nextv = next._1.future.value.get match{
        case Success(s) => if(s.nonEmpty) s.head
        else {
          proc(head._1.future.value.get.get)
          return true
        }
        case Failure(f) => f match{
          case n: NoMoreLinesException => {
            proc(head._1.future.value.get.get)
            return true
          }
          case z => throw z
        }
      }
      val spans = treeMap.tail.span(x => comp.compare(nextv, headv) > 0)
      treeMap = spans._1 ::: List((next._1, next._2)) ::: spans._2
      proc(head._1.future.value.get.get)
    }
    true
  }

  private def readToQuad(reader: BufferedLineReader): Option[Quad] = synchronized{
    if(reader.hasMoreLines) {
      var readerQuad: Quad = null
      while (reader.hasMoreLines && readerQuad == null)
        Quad.unapply(reader.readLine()) match {
          case Some(q) => readerQuad = q
          case None =>
        }
      Option(readerQuad)
    }
    else None
  }


  /**
   * @param tag for logging
   * @param file input file
   * @param proc process quad
   */
  def readQuads(tag: String, file: FileLike[_], until: Long = -1l)(proc: Quad => Unit): Boolean = {
    val dataset = "(?<=(.*wiki-\\d{8}-))([^\\.]+)".r.findFirstIn(file.toString) match {
      case Some(x) => x
      case None => null
    }
    if(until < 0 || this.reader == null)
      getRecorder.initialize(tag)

    this.reader = if(until > 0 && this.reader != null) this.reader else IOUtils.bufferedReader(file)
    this.file = if(until > 0 && this.reader != null) this.file else file
    val stopAt = if(until < 0) -1 else until + this.reader.getCharsRead
    
    try {
      reader.foreach{ line: String =>
        line match {
          case null => // ignore last value
          case Quad(quad) => {
            val copy = quad.copy (
              dataset = dataset
            )
            proc(copy)
            addQuadRecord(copy, tag)
          }
          case str => if (str.nonEmpty && !str.startsWith("#"))
            addQuadRecord(null, tag, null, new IllegalArgumentException("line did not match quad or triple syntax: " + line))
        }
        if(stopAt > 0 && reader.getCharsRead > stopAt) {
          throw new Exception("limit reached - we just break the try object")
        }
      } match{
        case Failure(f) if f.getMessage != null && f.getMessage.startsWith("limit reached") =>
          return false    // limit reached -> return without closing the reader
        case Failure(f) => throw f
        case _ =>
      }
    }
    finally
      if(until < 0)
        reader.close()

    addQuadRecord(null, tag, "reading quads completed with {page} quads", null)
    true
  }

  def closeReader() = if(this.reader != null) this.reader.close()

  private def logRead(tag: String, lines: Int, start: Long): Unit = {
    val micros = (System.nanoTime - start) / 1000
    err.println(tag+": read "+lines+" lines in "+ StringUtils.prettyMillis(micros / 1000)+" ("+(micros.toFloat / lines)+" micros per line)")
  }
}