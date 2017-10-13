package org.dbpedia.quad.processing

import java.io.Closeable
import java.util.concurrent.ArrayBlockingQueue

import org.dbpedia.quad.Quad
import org.dbpedia.quad.file.{BufferedLineReader, NoMoreLinesException}
import org.dbpedia.quad.sort.CodePointComparator
import org.dbpedia.quad.utils.FilterTarget

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.util.{Failure, Success}


/**
  * Created by chile on 27.08.17.
  */
class QuadGroupReader(val blr: BufferedLineReader, target: FilterTarget.Value, checkSorted: Boolean) extends Closeable with Iterator[Promise[Seq[Quad]]] {

  def this(blr: BufferedLineReader, target: FilterTarget.Value) = this(blr, target, false)
  def this(blr: BufferedLineReader) = this(blr, FilterTarget.subject, false)

  private val comparator = new CodePointComparator()
  private val ne: ArrayBlockingQueue[Promise[Seq[Quad]]] = new ArrayBlockingQueue[Promise[Seq[Quad]]](QuadGroupReader.QUEUESIZE)

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
      buffer
    }
    finally{
      blr.unlockReader(stamp)
    }
  }

  for(i <- 0 until QuadGroupReader.QUEUESIZE){
    try{
        ne.put(worker.work(null.asInstanceOf[String]))
    }
    catch{
      case e: Exception => ne.put(Promise.failed(e))
    }
  }

  def readGroup(): Promise[Seq[Quad]] ={
    try {
      pollAndPut()
    }
    catch{
      case e: Exception => Promise.failed(e)
    }
  }

  def readGroup(targetValue: String): Promise[Seq[Quad]] ={
    var ret: Promise[Seq[Quad]] = null
    try {
      ret = peekGroup() match{
        case Some(p) => p
        case None => Promise.failed(new NoMoreLinesException)
      }
      var compVal = resolvePromise(ret).headOption match{
        case Some(h) => comparator.compare(FilterTarget.resolveQuadResource(h, target), targetValue)
        case None => 1
      }
      while (compVal < 0) {
        pollAndPut()
        ret = peekGroup() match{
          case Some(p) => p
          case None => Promise.failed(new NoMoreLinesException)
        }
        compVal = resolvePromise(ret).headOption match{
          case Some(h) => comparator.compare(FilterTarget.resolveQuadResource(h, target), targetValue)
          case None => 1
        }
      }
      if(compVal > 0)
        ret = Promise.successful(Seq())     //we found no quad with the target value
      else
        ret = pollAndPut()
    }
    catch{
      case e: Exception => ret = Promise.failed(e)
    }
    ret
  }

  def peekGroup(): Option[Promise[Seq[Quad]]] = {
    if(ne.isEmpty)
      None
    else
      Some(ne.peek())
  }

  def linesRead(): Int = blr.getLineCount

  private def pollAndPut(): Promise[Seq[Quad]] = {
    var ret = ne.poll()
    ne.put(worker.work(null))
    if(ret == null)
      ret = Promise.failed(new QueueEmptyException)
    Await.ready(ret.future, Duration.Inf)
    ret
  }

  override def close(): Unit = blr.close()

  override def hasNext: Boolean = {
    peekGroup() match{
      case Some(x) =>
        Await.ready(x.future, Duration.Inf)
        x.future.value.get match{
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
        Await.ready(x.future, Duration.Inf)
        x.future.value.get match{
          case Success(s) => true
          case Failure(f) => false
        }
      case None => false
    }
  }

  override def next(): Promise[Seq[Quad]] = readGroup()

  private def resolvePromise(promise: Promise[Seq[Quad]]): Traversable[Quad] = {
    PromisedWork.awaitResults[Seq[Quad]](Seq(promise.future)).flatten
  }
}

object QuadGroupReader{
  private val QUEUESIZE = 1000

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