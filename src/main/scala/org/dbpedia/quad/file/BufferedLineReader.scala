package org.dbpedia.quad.file

import java.io.{BufferedReader, IOException, Reader}
import java.util.stream.Stream

import scala.util.Try

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

  override def readLine(): String = synchronized{
    if(noMoreLines)
      throw new NoMoreLinesException()

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

  private def nextLine(): Option[String] ={
    if (nextLineStr != null){
      val next = nextLineStr               //if setbackOneLine was called last, the next new line is int the nextLine variable, else it comes from the iterator
      nextLineStr = null
      Some(next)
    }
    else
      Option(super.readLine())
  }

  def getCharsRead: Long = charsRead

  override def lines(): Stream[String] = synchronized{java.util.stream.Stream.concat(java.util.stream.Stream.of("", currentLine), super.lines())}

  def peek: String = synchronized{currentLine}

  def getLineCount: Int = linecount

  def hasMoreLines: Boolean = !noMoreLines

  def setBackOneLine(): Boolean = synchronized{
    if(lastLine != null) {
      noMoreLines = false
      linecount = linecount - 1
      charsRead = charsRead -
        (if(lastLine != null)
          lastLine.getBytes("UTF-8").length else 0)
      nextLineStr = currentLine
      currentLine = lastLine
      lastLine = null
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
        return Try{}
    }
  }
}

class NoMoreLinesException extends IOException{
  override def getMessage: String = "This BufferedLineReader is out of lines (file finished)."
}