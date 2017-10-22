package org.dbpedia.quad.file

import java.io._
import java.net.URL
import java.nio.charset.Charset
import java.util.zip.{GZIPInputStream, GZIPOutputStream, Inflater, InflaterInputStream}

import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream}

import scala.io.Codec

/**
 * TODO: modify the bzip code such that there are no run-time dependencies on commons-compress.
 * Users should be able to use .gz files without having commons-compress on the classpath.
 * Even better, look for several different bzip2 implementations on the classpath...
 */
object IOUtils {

  /**
   * Map from file suffix (without "." dot) to output stream wrapper
   */
  val zippers: Map[String, (OutputStream) => OutputStream] = Map[String, OutputStream => OutputStream] (
    "gz" -> { new GZIPOutputStream(_) }, 
    "bz2" -> { new BZip2CompressorOutputStream(_) } 
  )
  
  /**
   * Map from file suffix (without "." dot) to input stream wrapper
   */
  val unzippers: Map[String, (InputStream) => InputStream] = Map[String, InputStream => InputStream] (
    "gz" -> { new GZIPInputStream(_) }, 
    "bz2" -> { new BZip2CompressorInputStream(_, true) } 
  )
  
  /**
   * use opener on file, wrap in un/zipper stream if necessary
   */
  private def open[T](file: StreamSourceLike[_], opener: StreamSourceLike[_] => T, wrappers: Map[String, T => T]): T = {
    val name = file.name
    val suffix = name.substring(name.lastIndexOf('.') + 1)
    wrappers.getOrElse(suffix, identity[T] _)(opener(file)) 
  }
  
  /**
   * open output stream, wrap in zipper stream if file suffix indicates compressed file.
   */
  def outputStream(file: StreamSourceLike[_], append: Boolean = false): OutputStream =
    open(file, _.outputStream(append), zippers)
  
  /**
   * open input stream, wrap in unzipper stream if file suffix indicates compressed file.
   */
  def inputStream(file: StreamSourceLike[_]): InputStream =
    open(file, _.inputStream(), unzippers)

  def estimateCompressionRatio(file: StreamSourceLike[_]): Double ={
    val compIn = inputStream(file)
    val array = new Array[Byte](1000000)
    compIn.read(array)

    val compressedBytes = compIn match{
      case bz2: BZip2CompressorInputStream =>
        val bos = new ByteArrayOutputStream()
        val bzout = new BZip2CompressorOutputStream(bos)
        bzout.write(array)
        bzout.finish()
        bzout.close()
        bos.close()
        bos.size()
      case gz: InflaterInputStream => Option(gz.getClass.getDeclaredField("inf")) match {  //TODO test this
        case Some(field) => field.get(gz).asInstanceOf[Inflater].getBytesWritten
        case None => 1d
      }
      case _ => 1d
    }
    compIn.close()
    1000000/compressedBytes
  }

  /**
    * a simple concatenation of files with bash - cat also allows for concatenating compressed files
    * @param files
    * @param outFile
    */
  def concatFile(files: Seq[FileLike[_]], outFile: FileLike[_]): Boolean = {
    var command = "cat "
    for(i <- files.indices)
      command += files(i).getFile.getAbsolutePath + " "
    command += "> " + outFile.getFile.getAbsolutePath
    val camArray = collection.JavaConversions.seqAsJavaList(List( "/bin/bash", "-c", command ))
    val pb = new ProcessBuilder(camArray)
    val ret = Integer.valueOf(pb.start().waitFor())
    ret == 0
  }

  def createStreamSource(uri: String): RichUrl ={
    Option(IOUtils.getClass.getClassLoader.getResource(uri)) match{
      case Some(u) => new RichUrl(u)
      case None if uri.matches("\\w+://(\\w|\\.)+.*") => new RichUrl(uri.trim)
      case None => new RichUrl("file:///" + (if(uri.startsWith("/")) uri.substring(1) else uri))
    }
  }

  /**
   * open output stream, wrap in zipper stream if file suffix indicates compressed file,
   * wrap in writer.
   */
  def writer(file: StreamSourceLike[_], append: Boolean = false, charset: Charset = Codec.UTF8.charSet): Writer =
    new OutputStreamWriter(outputStream(file, append), charset)
  
  /**
   * open input stream, wrap in unzipper stream if file suffix indicates compressed file,
   * wrap in reader.
   */
  def reader(file: StreamSourceLike[_], charset: Charset = Codec.UTF8.charSet): Reader =
    new InputStreamReader(inputStream(file), charset)

  def bufferedReader(file: StreamSourceLike[_], charset: Charset = Codec.UTF8.charSet): BufferedLineReader =
    new BufferedLineReader(new InputStreamReader(inputStream(file), charset))
  
  /**
   * open input stream, wrap in unzipper stream if file suffix indicates compressed file,
   * wrap in reader, wrap in buffered reader, process all lines. The last value passed to
   * proc will be null.
   */
  def readLines(file: StreamSourceLike[_], charset: Charset = Codec.UTF8.charSet)(proc: String => Unit): Unit = {
    val reader = this.bufferedReader(file)
    try {
      for (line <- reader) {
        proc(line)
      }
    }
    finally reader.close()
  }

  /**
   * Copy all bytes from input to output. Don't close any stream.
   */
  def copy(in: InputStream, out: OutputStream) : Unit = {
    val buf = new Array[Byte](1 << 20) // 1 MB
    while (true)
    {
      val read = in.read(buf)
      if (read == -1)
      {
        out.flush()
        return
      }
      out.write(buf, 0, read)
    }
  }
}
