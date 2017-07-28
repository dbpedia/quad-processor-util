package org.dbpedia.quad.destination

import java.io.File

import org.dbpedia.quad.file.{FileLike, IOUtils, RichFile}
import org.dbpedia.quad.formatters.Formatter

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chile on 14.06.17.
  */
object DestinationUtils {
  def createDestination(baseDir: FileLike[_], datasets: Seq[String], formats: Map[String, Formatter], appendHeader: String = null, filterParams: FilterParams = null) : Destination = {
    val destination = new ArrayBuffer[Destination]()
    for ((suffix, format) <- formats) {
      if(appendHeader != null)
        format.setHeader(format.header + appendHeader)
      for (dataset <- datasets) {
        destination += (if(filterParams == null)
              new WriterDestination(() => IOUtils.writer(new RichFile(new File(baseDir.getFile, dataset.replace('_', '-') + '.' + suffix))), format)
            else
              getDatasetDestination(baseDir, dataset, suffix, format, filterParams)
          )
      }
    }
    new CompositeDestination(destination.toSeq: _*)
  }

  def getWriterDestination(file: FileLike[_], format: Formatter): WriterDestination ={
    new WriterDestination(() => IOUtils.writer(file), format)
  }

  def getDatasetDestination(baseDir: FileLike[_], dataset: String, suffix: String, format: Formatter): FilterDestination = {
    val file = new RichFile(new File(baseDir.getFile, dataset.replace('_', '-') + '.' + suffix))
    file.getFile.createNewFile()
    new FilterDestination(dataset, new WriterDestination(() => IOUtils.writer(file), format))
  }

  def getDatasetDestination(baseDir: FileLike[_], dataset: String, suffix: String, format: Formatter, filterParams: FilterParams): FilterDestination = {
    if(filterParams == null)
      return getDatasetDestination(baseDir, dataset, suffix, format)
    val file = new RichFile(new File(baseDir.getFile, dataset.replace('_', '-') + '.' + suffix))
    file.getFile.createNewFile()
    new FilterDestination(filterParams, new WriterDestination(() => IOUtils.writer(file), format))
  }
}
