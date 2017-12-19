package org.dbpedia.quad.processing

import java.io.Closeable
import java.util.Comparator

import org.dbpedia.quad.Quad
import org.dbpedia.quad.file.{BufferedLineReader, NoMoreLinesException}
import org.dbpedia.quad.sort.CodePointComparator
import org.dbpedia.quad.utils.FilterTarget

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
  * Created by chile on 27.08.17.
  */
class QuadGroupReader(val blr: BufferedLineReader, target: FilterTarget.Value, checkSorted: Boolean) extends Closeable with Iterator[Future[Seq[Quad]]] {

  def this(blr: BufferedLineReader, target: FilterTarget.Value) = this(blr, target, false)
  def this(blr: BufferedLineReader) = this(blr, FilterTarget.subject, false)

  private val comparator = new CodePointComparator()
  private val tupleComp = new Comparator[Future[Seq[Quad]]] {
    var count = 0
    private def collectHead(f: Future[Seq[Quad]]): String ={
      f.value match{
        case Some(t) => t match {
          case Success(s: Seq[Quad]) if s.nonEmpty => FilterTarget.resolveQuadResource(s.head, target)
          case _ => null
        }
        case None => null
      }
    }
    val codepointComp = new CodePointComparator()
    override def compare(t: Future[Seq[Quad]], t1: Future[Seq[Quad]]) = {
      count = count +1
      PromisedWork.awaitResults(Seq(t,t1))
      val head1 = collectHead(t)
      val head2 = collectHead(t1)
      codepointComp.compare(head1, head2)
    }
  }

  private val worker = PromisedWork.apply[String, Seq[Quad]]{ until: String =>
    val buffer = new ListBuffer[Quad]()

    val stamp = blr.lockReader()

    try {
      var readerQuad: Option[Quad] = QuadGroupReader.readToQuad(blr, stamp)
      readerQuad match {
        case Some(x) =>
          val value = Option(until) match {
            case Some(u) => u
            case None => FilterTarget.resolveQuadResource(x, target)
          }

          while (readerQuad.isDefined && comparator.compare(FilterTarget.resolveQuadResource(readerQuad.get, target), value) < 0) {
            readerQuad = QuadGroupReader.readToQuad(blr, stamp)
          }
          while (readerQuad.isDefined && comparator.compare(FilterTarget.resolveQuadResource(readerQuad.get, target), value) == 0) {
            buffer.append(readerQuad.get)
            readerQuad = QuadGroupReader.readToQuad(blr, stamp)
          }
          //set back one line, else we will jump over one
          if(readerQuad.isDefined)
            blr.setBackOneLine(stamp)
        case None =>
      }
    }
    finally{
      blr.unlockReader(stamp)
    }
    buffer
  }

  private var peek: Future[Seq[Quad]] = worker.work(null.asInstanceOf[String]).future

  def readGroup(): Future[Seq[Quad]] ={
    try {
      pollAndPut()
    }
    catch{
      case e: Exception => Future.failed(e)
    }
  }

  def readGroup(targetValue: String): Future[Seq[Quad]] ={
    var ret: Future[Seq[Quad]] = null
    try {
      ret = peekGroup() match{
        case Some(p) => p
        case None => Future.failed(new NoMoreLinesException)
      }
      var compVal = resolvePromise(ret).headOption match{
        case Some(h) => comparator.compare(FilterTarget.resolveQuadResource(h, target), targetValue)
        case None => 1
      }
      while (compVal < 0) {
        pollAndPut()          //discard the current head (since it before the target values)
        ret = peekGroup() match{
          case Some(p) => p
          case None => Future.failed(new NoMoreLinesException)
        }
        compVal = resolvePromise(ret).headOption match{
          case Some(h) => comparator.compare(FilterTarget.resolveQuadResource(h, target), targetValue)
          case None => 1
        }
      }
      if(compVal > 0)
        ret = Future.successful(Seq())     //we found no quad with the target value
      else
        ret = pollAndPut()                //
    }
    catch{
      case e: Exception => ret = Future.failed(e)
    }
    ret
  }

  def peekGroup(): Option[Future[Seq[Quad]]] = {
    Option(this.peek)
  }

  def linesRead(): Int = blr.getLineCount

  private def pollAndPut(): Future[Seq[Quad]] = {
    val ret = peek
    this.peek = worker.work(null.asInstanceOf[String]).future
    Await.ready(ret, Duration.Inf)
    ret
  }

  override def close(): Unit = blr.close()

  override def hasNext: Boolean = {
    peekGroup() match{
      case Some(x) =>
        Await.ready(x, Duration.Inf)
        x.value.get match{
          case Success(s) => true
          case Failure(f) => f match {
            case t : NoMoreLinesException => false
            case _ => true
          }
        }
      case None => false
    }
  }

  def hasSuccessfullNext: Boolean ={
    peekGroup() match{
      case Some(x) =>
        Await.ready(x, Duration.Inf)
        x.value.get match{
          case Success(s) => true
          case Failure(f) => false
        }
      case None => false
    }
  }

  override def next(): Future[Seq[Quad]] = readGroup()

  private def resolvePromise(future: Future[Seq[Quad]]): Traversable[Quad] = {
    PromisedWork.awaitResults[Seq[Quad]](Seq(future)).flatten
  }
}

object QuadGroupReader{
  private val QUEUESIZE = 100

  /**
    * transforms a given line from a reader into a Quad, skips over non-quad lines (comments, empty etc.)
    * @param reader - the line reader
    * @param stamp - needed if reader is locked
    * @return
    */
  def readToQuad(reader: BufferedLineReader, stamp: Long = -1l): Option[Quad] = synchronized{
    var readerQuad: Option[Quad] = None
    while (reader.hasMoreLines && readerQuad.isEmpty)
      readerQuad = Quad.unapply(reader.readLine(stamp))
    readerQuad
  }
}

class QueueEmptyException extends Exception{
  override def getMessage: String = "Result queue is empty."
}