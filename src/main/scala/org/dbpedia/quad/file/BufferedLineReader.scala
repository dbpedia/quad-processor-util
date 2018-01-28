package org.dbpedia.quad.file

import java.io.{BufferedReader, IOException, Reader}
import java.util.concurrent.locks.StampedLock
import java.util.stream.Stream

import scala.util.{Failure, Success, Try}

/**
  * Created by chile on 07.06.17.
  */
class BufferedLineReader(reader: Reader) extends BufferedReader(reader){
  private var nextLineStr: String = _
  private var currentLine: String = _
  private var lastLine: String = _
  private var linecount: Int = 0
  private var noMoreLines = false
  private var charsRead = 0l
  private val accessLock = new StampedLock()

  def lockReader(): Long = accessLock.writeLock()

  def unlockReader(stamp: Long): Boolean = {
    if(stamp < 0)
      return false
    if(accessLock.isWriteLocked) {
      Try{accessLock.unlockWrite(stamp)} match{
        case Success(s) => true
        case Failure(f) => false
      }
    }
    else
      true
  }

  override def readLine(): String = readLine(-1l)

  def readLine(stamp: Long): String = {
    if(noMoreLines)
      throw new NoMoreLinesException()
    if(accessLock.isWriteLocked && !accessLock.validate(stamp))
      throw new IllegalArgumentException("This locked reader was not provided with a valid lock-stamp: " + stamp + " Please unlock or provide correct lock-stamp!")

    val ret = if(currentLine == null)
      super.readLine()                  //happens at the start of the file
    else
      currentLine

    lastLine = currentLine
    currentLine = nextLine() match {
      case Some(c) => c
      case None =>
        noMoreLines = true
        null
    }

    linecount = linecount +1
    charsRead += ret.getBytes("UTF-8").length
    ret
  }

  override def read(): Int = throw new UnsupportedOperationException("BufferedLineReader does only support reading lines!")
  override def read(chars: Array[Char], i: Int, i1: Int): Int = throw new UnsupportedOperationException("BufferedLineReader does only support reading lines!")
  override def skip(l: Long): Long = throw new UnsupportedOperationException("BufferedLineReader does only support reading lines!")
  override def markSupported(): Boolean = false

  private def nextLine(): Option[String] ={
    if (nextLineStr != null){
      val next = nextLineStr               //if setbackOneLine was called last, the next new line is in the nextLine variable, else it comes from the iterator
      nextLineStr = null
      Some(next)
    }
    else
      Option(super.readLine())
  }

  def getCharsRead: Long = charsRead

  override def lines(): Stream[String] = java.util.stream.Stream.concat(java.util.stream.Stream.of("", currentLine), super.lines())

  def peek: String = currentLine

  def getLineCount: Int = linecount

  def hasMoreLines: Boolean = !noMoreLines

  def setBackOneLine(stamp: Long = -1l): Boolean = {
    if(accessLock.isWriteLocked && !accessLock.validate(stamp))
      throw new IllegalArgumentException("This locked reader was not provided with a valid lock-stamp: " + stamp + " Please unlock or provide correct lock-stamp!")

    if(lastLine != null) {
      linecount = linecount - 1
      charsRead = charsRead -
        (if(lastLine != null)
          lastLine.getBytes("UTF-8").length else 0)
      nextLineStr = currentLine
      currentLine = lastLine
      lastLine = null
      noMoreLines = false
      return true
    }
    false
  }

  /**
    * Process all lines. The last value passed to proc will be null.
    */
  def foreach[U](proc: String => U): Try[Unit] = Try {
    while (true) {
      if (this.hasMoreLines)
        proc(this.readLine())
      else
        return Try(Unit)
    }
  }
}

class NoMoreLinesException extends IOException{
  override def getMessage: String = "This BufferedLineReader is out of lines (file finished)."
}