package org.dbpedia.quad.destination

import java.io.File

import org.dbpedia.quad.file.{IOUtils, RichFile}
import org.dbpedia.quad.formatters.Formatter
import org.dbpedia.quad.file.RichFile.wrapFile

/**
  * Created by chile on 25.08.17.
  */
class FileDestination(val file: File, format: Formatter)
  extends WriterDestination(() => IOUtils.writer(file), format){
  val richFile: RichFile = file
}
