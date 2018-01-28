package org.dbpedia.quad.file

import java.io.{InputStream, OutputStream}
import java.util.function.{Consumer, Predicate}

/**
  * Created by chile on 21.10.17.
  */
trait StreamSourceLike[T] {
  def name: String

  def inputStream(): InputStream

  def outputStream(append: Boolean = false): OutputStream

  def exists: Boolean

  def metadata: Option[StreamSourceMetaData] ={
    val br = IOUtils.bufferedReader(this)
    val line = br.lines().filter(new Predicate[String] {
      override def test(t: String): Boolean = t.trim.nonEmpty
    }).limit(10).findFirst()
    br.close()

    var ret: Option[StreamSourceMetaData] = None
    line.ifPresent(new Consumer[String] {
      override def accept(t: String): Unit = {
        ret =StreamSourceMetaData.apply(t)
      }
    })
    ret
  }
}
