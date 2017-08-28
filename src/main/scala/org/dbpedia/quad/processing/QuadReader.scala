package org.dbpedia.quad.processing

import org.dbpedia.quad.Quad
import org.dbpedia.quad.file.{BufferedLineReader, FileLike, IOUtils}
import org.dbpedia.quad.log.{LogRecorder, RecordEntry, RecordSeverity}
import org.dbpedia.quad.sort.QuadComparator
import org.dbpedia.quad.utils.{FilterTarget, StringUtils}

import scala.Console.err
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
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
  private var file: FileLike[_] = _

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

    val readers = files.map(x => new QuadGroupReader(IOUtils.bufferedReader(x)))

    val ret = readSortedQuads[T](tag, leadFile){ quads =>
      val subj = quads.head.subject
      val futureQuads = for (worker <- readers)
        yield worker.readGroup(subj)

      PromisedWork.waitAll(futureQuads)
      val zw = futureQuads.map(x => x.future.value).map(y => y.getOrElse(Try{Seq()}).getOrElse(Seq())).flatten  //TODO make this more readable and insert recovery!
      proc(zw ++ quads)
    }
    readers.foreach(_.close())
    ret
  }

  def readSortedQuads (tag:String, files: Seq[FileLike[_]], target: FilterTarget.Value = FilterTarget.subject)(proc: Traversable[Quad] => Unit): Unit = {
    val readers = files.map(x => new QuadGroupReader(IOUtils.bufferedReader(x), target))
    val comp = new QuadComparator(target)

    this.getRecorder.initialize(tag, "reading quads")

    var startup :List[(Promise[Seq[Quad]], QuadGroupReader)] = List()
    for (reader <- readers)
      startup = startup ::: List((reader.readGroup(), reader))

    PromisedWork.waitAll(startup.map(x => x._1.future))

    var treeMap :List[(Seq[Quad], QuadGroupReader)] = startup
      .sortWith((x,y) => comp.compare(x._1.future.value.get.get.head, y._1.future.value.get.get.head) < 0)
      .map(x => (x._1.future.value.getOrElse(Try{Seq()}).getOrElse(Seq()), x._2))

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
        Await.ready(next._1.future, Duration.Inf)
        val nextv = next._1.future.value.get match {
          case Success(s) => s
          case Failure(f) => f match {
            case b => throw b
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
  }

  /**
   * @param tag for logging
   * @param file input file
   * @param proc process quad
   */
  def readQuads(tag: String, file: FileLike[_], until: Long = -1l)(proc: Quad => Unit): Boolean = {
    val dataset = "(?<=(.*wiki-\\d{8}-))([^\\.]+)".r.findFirstIn(file.toString) match {
      case Some(x) => Seq(x)
      case None => Seq()
    }
    if(until < 0 || this.reader == null)
      getRecorder.initialize(tag, "reading quads", dataset)

    this.reader = if(until > 0 && this.reader != null) this.reader else IOUtils.bufferedReader(file)
    this.file = if(until > 0 && this.reader != null) this.file else file
    val stopAt = if(until < 0) -1 else until + this.reader.getCharsRead
    
    try {
      reader.foreach{ line: String =>
        line match {
          case null => // ignore last value
          case Quad(quad) =>
            val copy = quad.copy (
              dataset = if(dataset.nonEmpty) dataset.head else null
            )
            proc(copy)
            addQuadRecord(copy, tag)
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

  def closeReader(): Unit = if(this.reader != null) this.reader.close()

  private def logRead(tag: String, lines: Int, start: Long): Unit = {
    val micros = (System.nanoTime - start) / 1000
    err.println(tag+": read "+lines+" lines in "+ StringUtils.prettyMillis(micros / 1000)+" ("+(micros.toFloat / lines)+" micros per line)")
  }
}