package org.dbpedia.quad.processing

import java.util.concurrent.ArrayBlockingQueue

import org.dbpedia.quad.processing.Workers.defaultThreads

import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Created by chile on 15.06.17.
  */

class PromiseIterator[R](factory: => Worker[Unit, R], availThreads: Int = defaultThreads , queueLength: Int = defaultThreads)
  extends PromisedWork[Unit, R](factory) with Iterator[Promise[R]]{

  private val ne: ArrayBlockingQueue[Promise[R]] = new ArrayBlockingQueue[Promise[R]](1000)

  for(i <- 0 until 1000)
    ne.put(this.work(Unit))

  override def hasNext: Boolean = !ne.isEmpty

  final override def next(): Promise[R] = {
    val ret = ne.poll()
    ne.put(this.work(Unit))
    ret
  }
}

object PromiseIterator{

  def wrapPromisedWorker[R](pm: PromisedWork[Unit, R]): PromiseIterator[R] = new PromiseIterator[R](pm.getFactory, pm.availThreads, pm.queueLength)

  /**
    * Convenience 'constructor'. Allows very concise syntax:
    * val workers = SimpleWorkers(threads, queueLength) { foo: Foo =>
    *   // do something with foo...
    * }
    */

  def apply[R](threads: Int, queueLength: Int)(proc: Unit => R): PromiseIterator[R] = {
    wrapPromisedWorker(PromisedWork(threads, queueLength){
      v:Unit => proc(Unit)
    }
    )
  }


  def byFuture[R](threads: Int, queueLength: Int)(proc: Unit => Future[R]): PromiseIterator[R] = {
    wrapPromisedWorker(PromisedWork.byFuture(threads, queueLength){
      v:Unit => proc(Unit)
    }
    )
  }

  /**
    * Convenience 'constructor'. Allows very concise syntax:
    * val workers = SimpleWorkers(loadFactor, queueDepth) { foo: Foo =>
    *
    * // do something with foo...
    * }
    */
  def apply[R](loadFactor: Double, queueDepth: Double)(proc: Unit => R): PromiseIterator[R] = {
    apply[R]((defaultThreads * loadFactor).toInt, (defaultThreads * loadFactor * queueDepth).toInt)(proc)
  }

  /**
    * Convenience 'constructor'. Allows very concise syntax:
    * val workers = SimpleWorkers { foo: Foo =>
    *   // do something with foo...
    * }
    */
  def apply[R](proc: Unit => R): PromiseIterator[R] = {
    apply[R](defaultThreads, defaultThreads)(proc)
  }
}