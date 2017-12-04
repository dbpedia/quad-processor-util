package org.dbpedia.quad.scripts

import org.dbpedia.quad.Quad
import org.dbpedia.quad.config.Config
import org.dbpedia.quad.destination.{Destination, DestinationUtils}
import org.dbpedia.quad.file.IOUtils
import org.dbpedia.quad.processing.{QuadMapper, QuadReader}
import org.dbpedia.quad.utils.FilterTarget

/**
  * Can be used to read or map multiple quad input files, which need to be code-point sorted (see QuadSorter).
  * @param config - the configuration file in use.
  */
abstract class SortedQuadTraversal(config: Config) {

  val target: FilterTarget.Value = FilterTarget.subject

  private val leadFile = config.getArbitraryStringProperty("primary-input-dataset") match{
    case Some(f) => IOUtils.createStreamSource(config.dumpDir.toString + "/" + f)
    case None => throw new IllegalArgumentException("Property primary-input-dataset was not found in the provided properties file.")
  }
  private val sortedInputFiles = config.inputDatasets.map(s => IOUtils.createStreamSource(config.dumpDir.toString + "/" + s))
  private val destination: Destination = config.outputDataset match {
    case Some(s) => DestinationUtils.createDestination(config.dumpDir, Seq(s), config.formats.toMap)
    case None => null
  }


  /**
    * This function provides the mapping function for the quads read. The sequence of Quads forwarded here, contains all quads with the same target positions(subj, pred, obj) over all input files.
    * @param quads
    * @return
    */
  def process(quads: Traversable[Quad]): Traversable[Quad]

  /**
    * This method will read the given input files and provide the input for the process function. The results of the process function are redirected to the specified destination.
    * @param dest - if provided all output quads are redirected into this destination, else the destination specified in the properties file is used
    */
  def startMapping(dest: Destination = null): Unit = {
    if(dest != null)
      new QuadMapper().mapSortedQuads(config.tag, leadFile, sortedInputFiles, target, dest){ quads => process(quads)}
    else if(destination != null)
      new QuadMapper().mapSortedQuads(config.tag, leadFile, sortedInputFiles, target, destination){ quads => process(quads)}
    else
      throw new IllegalArgumentException("No destination was specified (with property 'output' or as parameter).")
  }

  /**
    * This method will read the given input files and provide the input for the process function. The results of the process function are discarded (just return Seq()).
    */
  def startReading(): Unit = new QuadReader().readSortedQuads(config.tag, leadFile, sortedInputFiles,  target){ quads => process(quads)}
}

object SortedQuadTraversal{
  /**
    * This main demonstrates how to apply a SortedQuadTraversal
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val config = new Config(args(0))

    val sqt = new SortedQuadTraversal(config) {
      override def process(quads: Traversable[Quad]): Traversable[Quad] = {
        // map quads here
        // return new seq of quads which will be saved in the destination file
        quads
      }
    }

    sqt.startReading()
  }
}
