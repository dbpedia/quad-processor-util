package org.dbpedia.quad.destination
import org.dbpedia.quad.Quad
import org.dbpedia.quad.formatters.Formatter
import org.dbpedia.quad.utils.FilterTarget

/**
  * Created by chile on 14.06.17.
  */
class FilterDestination(val params: FilterParams, target: Destination) extends Destination {

  def this(f: String, ft: FilterTarget.Value, target: Destination){
    this(ft match{
      case FilterTarget.subject => FilterParams(f, null, null, null, null)
      case FilterTarget.predicate => FilterParams(null, f, null, null, null)
      case FilterTarget.value => FilterParams(null, null, f, null, null)
      case FilterTarget.graph => FilterParams(null, null, null, f, null)
    }, target)
  }

  def this(set: String, target: Destination){
    this(FilterParams(null, null, null, null, set), target)
  }

  val filter = new DestinationFilter(params)
  /**
    * Opens this destination. This method should only be called once during the lifetime
    * of a destination, and it should not be called concurrently with other methods of this class.
    */
  override def open(): Unit = target.open()

  /**
    * Writes quads to this destination. Implementing classes should make sure that this method
    * can safely be executed concurrently by multiple threads.
    */
  override def write(graph: Traversable[Quad]): Unit = {
    val zw = for(quad <- graph) yield filter.filter(quad)
    target.write(zw.collect{case Some(x) => x})
  }

  /**
    * Closes this destination. This method should only be called once during the lifetime
    * of a destination, and it should not be called concurrently with other methods of this class.
    */
  override def close(): Unit = target.close()

  /**
    * provide information about the intended format (syntax) of the destination file
    */
  override val formatter: Formatter = target.formatter
}

class DestinationFilter(params: FilterParams){
  def filter(quad: Quad): Option[Quad] ={

    if(params.dataset != null) Option(quad.dataset) match{
      case Some(n) if n == params.dataset =>
      case _ => return None
    }

    if(params.subjectFilter.nonEmpty && !params.subjectFilter.contains(quad.subject))
      return None
    if(params.predicateFilter.nonEmpty && !params.predicateFilter.contains(quad.predicate))
      return None
    else if(params.objectFilter.nonEmpty && !params.objectFilter.contains(quad.value))
      return None
    if(quad.context != null && params.graphFilter.nonEmpty && !params.graphFilter.contains(quad.context))
      return None

    Some(quad)
  }
}

case class FilterParams(subFilter: String, predFilter: String, objFilter: String, graFilter: String, dataset: String){
  val subjectFilter: Seq[String] = if(subFilter == null || subFilter.isEmpty) Seq() else subFilter.split(",").map(_.trim).toSeq
  val predicateFilter: Seq[String] = if(predFilter == null || predFilter.isEmpty) Seq() else predFilter.split(",").map(_.trim).toSeq
  val objectFilter: Seq[String] = if(objFilter == null || objFilter.isEmpty) Seq() else objFilter.split(",").map(_.trim).toSeq
  val graphFilter: Seq[String] = if(graFilter == null || graFilter.isEmpty) Seq() else graFilter.split(",").map(_.trim).toSeq
}
