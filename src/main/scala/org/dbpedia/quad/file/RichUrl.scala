package org.dbpedia.quad.file
import java.io.{File, InputStream, OutputStream}
import java.net.URL

import scala.util.{Success, Try}

/**
  * Created by chile on 21.10.17.
  */
class RichUrl(url: URL) extends StreamSourceLike[URL]{

  def this(uri: String) = this(new URL(uri))

  override def name: String = url.getFile

  override def inputStream(): InputStream = url.openStream()

  override def outputStream(append: Boolean): OutputStream = url.openConnection().getOutputStream

  override def exists: Boolean = {
    Try{url.openConnection()} match{
      case Success(_) => true
      case _ => false
    }
  }

  def getURL: URL = url
}

object RichUrl{

  implicit def wrapUrl(file: URL): RichUrl = new RichUrl(file)

  implicit def toUrl(file: String): URL = new URL(file)

  implicit def fromFile(file: File): RichUrl = file.toURI.toURL
}