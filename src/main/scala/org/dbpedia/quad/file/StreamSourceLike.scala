package org.dbpedia.quad.file

import java.io.{InputStream, OutputStream}

/**
  * Created by chile on 21.10.17.
  */
trait StreamSourceLike {
  def name: String

  def inputStream(): InputStream

  def outputStream(append: Boolean = false): OutputStream

  def exists: Boolean
}
