package org.dbpedia.quad.file

import java.io.{BufferedReader, Reader}

import scala.language.implicitConversions

object RichReader
{
  implicit def wrapReader(reader: BufferedReader) = new RichReader(reader)
  
  implicit def wrapReader(reader: Reader) = new RichReader(reader)
}


class RichReader(reader: BufferedReader) {
  
  def this(reader: Reader) = this(new BufferedReader(reader))
  
  /**
   * Process all lines. The last value passed to proc will be null. 
   */
  def foreach[U](proc: String => U): Unit = {
    while (true) {
      val line = reader.readLine()
      proc(line)
      if (line == null) return
    }
  }
}