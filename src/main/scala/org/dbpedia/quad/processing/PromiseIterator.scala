package org.dbpedia.quad.processing

import java.util.concurrent.ArrayBlockingQueue

import org.dbpedia.quad.processing.Workers.defaultThreads

import scala.concurrent.{Future, Promise}

/**
  * Created by chile on 15.06.17.
  */

class PromiseIterator[S, R](factory: => Worker[S, R], val source: S, availThreads: Int = defaultThreads , queueLength: Int = defaultThreads)
  extends PromisedWork[S, R](factory) with Iterator[Promise[R]]{

  private val ne: ArrayBlockingQueue[Promise[R]] = new ArrayBlockingQueue[Promise[R]](1000)

  for(i <- 0 until 1000)
    ne.put(this.work(source))

  override def hasNext: Boolean = !ne.isEmpty

  final override def next(): Promise[R] = {
    val ret = ne.poll()
    ne.put(this.work(source))
    ret
  }
}

object PromiseIterator{

  def wrapPromisedWorker[S, R](pm: PromisedWork[S, R], source: S): PromiseIterator[S,R] = new PromiseIterator[S,R](pm.getFactory, source, pm.availThreads, pm.queueLength)

  /**
    * Convenience 'constructor'. Allows very concise syntax:
    * val workers = SimpleWorkers(threads, queueLength) { foo: Foo =>
    *   // do something with foo...
    * }
    */

  def apply[S, R](source: S, threads: Int, queueLength: Int)(proc: S => R): PromiseIterator[S, R] = {
    wrapPromisedWorker(PromisedWork(threads, queueLength){
      s: S => proc(s)
    }, source
    )
  }


  def byFuture[S, R](source: S, threads: Int, queueLength: Int)(proc: S => Future[R]): PromiseIterator[S, R] = {
    wrapPromisedWorker(PromisedWork.byFuture(threads, queueLength){
      s: S => proc(s)
    }, source
    )
  }

  /**
    * Convenience 'constructor'. Allows very concise syntax:
    * val workers = SimpleWorkers(loadFactor, queueDepth) { foo: Foo =>
    *
    * // do something with foo...
    * }
    */
  def apply[S, R](source: S, loadFactor: Double, queueDepth: Double)(proc: S => R): PromiseIterator[S, R] = {
    apply[S, R](source, (defaultThreads * loadFactor).toInt, (defaultThreads * loadFactor * queueDepth).toInt)(proc)
  }

  /**
    * Convenience 'constructor'. Allows very concise syntax:
    * val workers = SimpleWorkers { foo: Foo =>
    *   // do something with foo...
    * }
    */
  def apply[S, R](source: S, proc: S => R): PromiseIterator[S, R] = {
    apply[S, R](source, defaultThreads, defaultThreads)(proc)
  }
}