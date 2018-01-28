package org.dbpedia.quad.processing

import java.io.IOException

import org.dbpedia.quad.Quad
import org.dbpedia.quad.file._
import org.dbpedia.quad.log.{LogRecorder, RecordEntry, RecordSeverity}
import org.dbpedia.quad.sort.QuadComparator
import org.dbpedia.quad.utils.FilterTarget

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.languageFeature.implicitConversions
import scala.util.{Failure, Success, Try}

/**
 */
class QuadReader(rec: LogRecorder[Quad]) {

  def this(log: FileLike[_] = null, reportInterval: Int = 100000, preamble: String = null) = {
    this(LogRecorder.create[Quad](log, reportInterval, preamble))
  }

  private val recorder: LogRecorder[Quad] = rec
  private var reader: BufferedLineReader = _
  private var file: StreamSourceLike[_] = _

  def this(){
    this(null, 100000, null)
  }

  def getRecorder: LogRecorder[Quad] = recorder

  def addQuadRecord(quad: Quad, lang: String, errorMsg: String = null, error: Throwable = null): Unit ={
    if(errorMsg == null && error == null)
      recorder.record(new RecordEntry[Quad]("", "", quad, RecordSeverity.Info, lang, errorMsg, error))
    else if(error != null)
      recorder.record(new RecordEntry[Quad]("", "", quad, RecordSeverity.Exception, lang, errorMsg, error))
    else
      recorder.record(new RecordEntry[Quad]("", "", quad, RecordSeverity.Warning, lang, errorMsg, error))
  }

  def readSortedQuads[T <% StreamSourceLike[T]](tag: String, file: StreamSourceLike[_], target: FilterTarget.Value)(proc: Traversable[Quad] => Unit): Boolean = {
    //TODO needs extraction-recorder syntax!
    var lastSubj = ""
    var seq = ListBuffer[Quad]()
    val ret = readQuads(tag, file) { quad =>
      val value = FilterTarget.resolveQuadResource(quad, target)
      if(!lastSubj.equals(value))
      {
        lastSubj = value
        if(seq.nonEmpty)
          proc(seq.toList)
        seq.clear()
      }
      seq += quad
    }
    if(seq.nonEmpty)
      proc(seq.toList)
    ret
  }

  def readSortedQuads[T <% StreamSourceLike[T]](tag:String, leadFile: StreamSourceLike[_], files: Seq[StreamSourceLike[_]], target: FilterTarget.Value)(proc: Traversable[Quad] => Unit): Boolean = {

    val readers = files.map(x => new QuadGroupReader(IOUtils.bufferedReader(x), target))

    val ret = readSortedQuads[T](tag, leadFile, target){ quads =>
      val subj = FilterTarget.resolveQuadResource(quads.head, target)
      val futureQuads = for (worker <- readers)
        yield worker.readGroup(subj)

      PromisedWork.awaitResults(futureQuads)
      val otherGroupsQuads = futureQuads.map(x => x.value).map {
        case Some(s) => s match {
          case Success(su) => su
          case Failure(f) => f match {
            case nml: NoMoreLinesException => Seq()
            case _ =>
              f.printStackTrace()
              Seq()
          }
        }
        case None => Seq()
      }
      proc(quads ++ otherGroupsQuads.flatten)
    }
    readers.foreach(_.close())
    ret
  }

  def readSortedQuads (tag:String, files: Seq[StreamSourceLike[_]], target: FilterTarget.Value, knownPrefix: String = null)(proc: Traversable[Quad] => Unit): Boolean = {
    val readers = files.map(x => new QuadGroupReader(IOUtils.bufferedReader(x), target))
    val comp = new QuadComparator(target, knownPrefix)

    this.getRecorder.initialize(tag, "reading quads")

    var startup :List[(Future[Seq[Quad]], QuadGroupReader)] = List()
    for (reader <- readers)
      startup = startup ::: List((reader.readGroup(), reader))

    PromisedWork.awaitResults(startup.map(x => x._1))

    //TODO needs to be better safeguarded against Failures!
    var treeMap :List[(Seq[Quad], QuadGroupReader)] = startup.filter(p => PromisedWork.isCompletedSuccessfully(p._1))
      .sortWith((x,y) => comp.compare(x._1.value.get.get.head, y._1.value.get.get.head) < 0)
      .map(x => (x._1.value.getOrElse(Try{Seq()}).getOrElse(Seq()), x._2))

    var procParam = new ListBuffer[Quad]()

    def appendAndExecute(quads: Seq[Quad]): Unit ={
      if(quads.isEmpty)
        return
      if(procParam.nonEmpty && comp.compare(procParam.head, quads.head) == 0)
        procParam.appendAll(quads)
      else{
        if(procParam.nonEmpty) {
          this.recorder.record(procParam.map(x => new RecordEntry[Quad](String.valueOf(x.hashCode()), x.subject, x, RecordSeverity.Info, "quad")): _*)
          proc(procParam)
        }
        procParam = new ListBuffer[Quad]()
        procParam.appendAll(quads)
      }
    }

    while (treeMap.nonEmpty) {
      val head = treeMap.head
      appendAndExecute(head._1)

      if(head._2.hasNext) {
        val next = (head._2.next(), head._2)
        Await.ready(next._1, Duration.Inf)
        val nextv = next._1.value.get match {
          case Success(s) => s
          case Failure(f) => f match{
            case n : NoMoreLinesException => Seq() //TODO
            case a => throw a
          }
        }

        if(nextv.nonEmpty) {
          val spans = treeMap.tail.span(x => comp.compare(nextv.head, x._1.head) > 0)
          treeMap = spans._1 ::: List((nextv, next._2)) ::: spans._2
        }
        else
          treeMap = treeMap.tail
      }
      else
        treeMap = treeMap.tail
    }
    if(procParam.nonEmpty)
      proc(procParam)

    readers.foreach(_.close())
    true
  }

  /**
   * @param tag for logging
   * @param file input file
   * @param proc process quad
   */
  def readQuads(tag: String, file: StreamSourceLike[_], until: Long = -1l)(proc: Quad => Unit): Boolean = {
    if(until < 0 || this.reader == null)
      getRecorder.initialize(tag, "reading quads", Seq(file.name))

    this.reader = if(until > 0 && this.reader != null) this.reader else IOUtils.bufferedReader(file)
    this.file = if(until > 0 && this.reader != null) this.file else file
    val stopAt = if(until < 0) -1 else until + this.reader.getCharsRead
    
    try {
      reader.foreach{ line: String =>
        line match {
          case null => // ignore last value
          case Quad(quad) =>
            proc(quad)
            addQuadRecord(quad, tag)
          case str => if (str.nonEmpty && !str.startsWith("#"))
            addQuadRecord(null, tag, null, new IllegalArgumentException("line did not match quad or triple syntax: " + line))
        }
        if(stopAt > 0 && reader.getCharsRead > stopAt) {
          throw new ReaderLimitReachedException
        }
      } match{
        case Failure(f) if f.isInstanceOf[ReaderLimitReachedException] => return false  // byte limit reached -> return without closing the reader
        case Failure(f) => throw f
        case Success(_) =>                                                              // the BufferedLineReader has reached EOF -> continue to finally and close the reader
      }
    }
    finally {
      if (until < 0) {
        closeReader()
        addQuadRecord(null, tag, "reading quads completed with {page} quads", null)
      }
    }
    true
  }

  def closeReader(): Unit = if(this.reader != null) this.reader.close()

  class ReaderLimitReachedException extends IOException{
    override def getMessage: String = "The defined byte limit of this QuadReader is reached. To read on, call the same method with the until parameter > 0."
  }
}