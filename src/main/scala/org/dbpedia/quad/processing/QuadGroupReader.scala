package org.dbpedia.quad.processing

import java.io.Closeable
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.locks.ReentrantLock

import org.dbpedia.quad.Quad
import org.dbpedia.quad.file.{BufferedLineReader, NoMoreLinesException}
import org.dbpedia.quad.sort.CodePointComparator
import org.dbpedia.quad.utils.FilterTarget

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.util.{Failure, Success, Try}

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
    val stamp = Await.result(blr.lockReader(), Duration.Inf)

    try {
      var readerQuad: Try[Quad] = QuadGroupReader.readToQuad(blr, stamp)
      readerQuad match {
        case Success(x) => {
          val value = Option(until) match {
            case Some(u) => u
            case None => FilterTarget.resolveQuadResource(x, target)
          }

          while (readerQuad.isSuccess && comparator.compare(FilterTarget.resolveQuadResource(readerQuad.get, target), value) < 0) {
            readerQuad = QuadGroupReader.readToQuad(blr, stamp)
          }
          while (readerQuad.isSuccess && comparator.compare(FilterTarget.resolveQuadResource(readerQuad.get, target), value) == 0) {
            buffer.append(readerQuad.get)
            readerQuad = QuadGroupReader.readToQuad(blr, stamp)
          }
          //set back one line, else we will jump over one
          if(readerQuad.isSuccess)
            blr.setBackOneLine(stamp)
        }
        case Failure(_) =>
      }
      buffer
    }
    finally{
      blr.unlockReader(stamp)
    }
  }

  private val lock = new ReentrantLock()

  for(i <- 0 until QuadGroupReader.QUEUESIZE){
    lock.lock()
    try{
        val ze = worker.work(null.asInstanceOf[String])
        Await.ready(ze.future, Duration.Inf)
        ne.put(ze)
    }
    catch{
      case e: Exception => ne.put(Promise.failed(e))
    }
    finally {
      lock.unlock()
    }
  }

  def readGroup(): Promise[Seq[Quad]] ={
    lock.lock()
    var ret: Promise[Seq[Quad]] = null
    try {
      ret = pollAndPut()
    }
    catch{
      case e: Exception => ret = Promise.failed(e)
    }
    finally {
      lock.unlock()
    }
    ret
  }

  def readGroup(targetValue: String): Promise[Seq[Quad]] ={
    lock.lock()
    var ret: Promise[Seq[Quad]] = null
    try {
      ret = pollAndPut()
      while (comparator.compare(FilterTarget.resolveQuadResource(QuadGroupReader.resolvePromise(ret).head, target), targetValue) < 0) {
        ret = pollAndPut()
      }
    }
    catch{
      case e: Exception => ret = Promise.failed(e)
    }
    finally {
      lock.unlock()
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
    val ret = ne.poll()
    ne.put(worker.work(null))
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
}

object QuadGroupReader{
  private val QUEUESIZE = 1000

  def readToQuad(reader: BufferedLineReader, stamp: Long = -1l): Try[Quad] = synchronized{
    Try {
      var readerQuad: Quad = null
      while (readerQuad == null)
        readerQuad = Quad.unapply(reader.readLine(stamp)) match {
          case Some(q) => q
          case None => null
        }
      readerQuad
    }
  }

  def resolvePromise(promise: Promise[Seq[Quad]]): Seq[Quad] = promise.future.value.get match{
    case Success(s) => s
    case Failure(f) =>
      f.printStackTrace()
      Seq()
  }
}