package org.dbpedia.quad.scripts

import org.dbpedia.quad.Quad
import org.dbpedia.quad.config.Config
import org.dbpedia.quad.destination.{Destination, DestinationUtils}
import org.dbpedia.quad.file.IOUtils
import org.dbpedia.quad.processing.QuadMapper
import org.dbpedia.quad.utils.FilterTarget

abstract class SortedQuadTraversal(config: Config) {

  val target: FilterTarget.Value = FilterTarget.subject

  private val leadFile = config.getArbitraryStringProperty("primary-input-dataset") match{
    case Some(f) => IOUtils.createStreamSource(config.dumpDir.toString + "/" + f)
    case None => throw new IllegalArgumentException("Property primary-input-dataset was not found in the provided properties file.")
  }
  private val sortedInputFiles = config.inputDatasets.map(s => IOUtils.createStreamSource(config.dumpDir.toString + "/" + s))
  private val destination: Destination = DestinationUtils.createDestination(config.dumpDir, Seq(config.outputDataset.getOrElse("")), config.formats.toMap)

  def process(quads: Traversable[Quad]): Traversable[Quad]

  def startMapping(): Unit = new QuadMapper().mapSortedQuads(config.tag, leadFile, sortedInputFiles, target, destination){ quads => process(quads)}
}

object SortedQuadTraversal{
  def main(args: Array[String]): Unit = {

    val config = new Config(args(0))
    var count = 0

    val test = new SortedQuadTraversal(config) {
      override def process(quads: Traversable[Quad]): Traversable[Quad] = {
        if(quads.exists(q => q.predicate == "http://scigraph.springernature.com/ontologies/core/language")){
          count += 1
          quads.map(q =>
            q.copy(subject = q.subject + "#test"))

        }
        Seq()
      }
    }

    test.startMapping()

    System.out.println(count + " abstracts found")
  }
}
