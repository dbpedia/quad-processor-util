package org.dbpedia.quad.file
import java.io.{InputStream, OutputStream}
import java.net.URL

import scala.util.{Success, Try}

/**
  * Created by chile on 21.10.17.
  */
class RichUrl(url: URL) extends StreamSourceLike{

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
}
