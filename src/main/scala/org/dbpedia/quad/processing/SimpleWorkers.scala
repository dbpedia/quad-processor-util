package org.dbpedia.quad.processing

import org.dbpedia.quad.processing.Workers._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by chile on 11.06.17.
  */
object SimpleWorkers {

  /**
   * Convenience 'constructor'. Allows very concise syntax:
   * val workers = SimpleWorkers(threads, queueLength) { foo: Foo =>
   *   // do something with foo...
   * }
   */

  def apply[T, R](threads: Int, queueLength: Int)(proc: T => R): PromisedWork[T, R] = {
    new PromisedWork[T, R](threads, queueLength, new Worker[T, R]() {
      private var state : WorkerState.Value = WorkerState.declared
      def init() = {state = WorkerState.initialized}
      def process(value: T)(implicit ec: ExecutionContext) = Future.apply(proc(value))(ec)
      def destroy() = {state = WorkerState.destroyed}
      def getState: org.dbpedia.quad.processing.WorkerState.Value = state
    })
  }

  /**
   * Convenience 'constructor'. Allows very concise syntax:
   * val workers = SimpleWorkers(loadFactor, queueDepth) { foo: Foo =>
   *   // do something with foo...
   * }
   */
  def apply[T, R](loadFactor: Double, queueDepth: Double)(proc: T => R): PromisedWork[T, R] = {
    apply[T, R]((defaultThreads * loadFactor).toInt, (defaultThreads * loadFactor * queueDepth).toInt)(proc)
  }

  /**
   * Convenience 'constructor'. Allows very concise syntax:
   * val workers = SimpleWorkers { foo: Foo =>
   *   // do something with foo...
   * }
   */
  def apply[T, R](proc: T => R): PromisedWork[T, R] = {
    apply[T, R](defaultThreads, defaultThreads)(proc)
  }
}
