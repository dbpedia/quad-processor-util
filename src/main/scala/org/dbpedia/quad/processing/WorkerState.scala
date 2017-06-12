package org.dbpedia.quad.processing

/**
  * Provides the overall state of a worker
  */
object WorkerState extends Enumeration {
  val
  declared,             //worker was declared but was not yet initialized (started)
  initialized,          //worker is ready to work after its initialization (started)
  destroyed = Value     //worker has finished all work after it was told to do so (destroyed)
}
