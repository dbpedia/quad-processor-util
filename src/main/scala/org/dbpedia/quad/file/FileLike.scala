package org.dbpedia.quad.file

import java.io.{File, InputStream, OutputStream}

import scala.util.Try

/**
 * Allows common handling of java.io.File and java.nio.file.Path
 */
trait FileLike[T] extends StreamSourceLike[T]{

  /**
   * @return full path
   */
  def toString: String

  /**
   * @return file name, or null if file path has no parts
   */

  def resolve(name: String): Try[T]

  def names: List[String]

  def list: List[T]

  @throws[java.io.IOException]("if file does not exist or cannot be deleted")
  def delete(recursive: Boolean = false): Unit

  def size(): Long

  def isFile: Boolean

  def isDirectory: Boolean

  def hasFiles: Boolean

  def getFile: File
}
