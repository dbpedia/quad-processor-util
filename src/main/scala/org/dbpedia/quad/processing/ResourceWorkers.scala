package org.dbpedia.quad.processing

import org.dbpedia.quad.processing.Workers._

/**
  * Created by chile on 11.06.17.
  */
object ResourceWorkers {

  /**
   * Convenience 'constructor'. Allows very concise syntax:
   * val workers = ResourceWorkers(threads, queueLength) {
   *   new Worker[Foo] {
   *     def init() = { /* init... */ }
   *     def process(value: T) = { /* work... */ }
   *     def destroy() = { /* destroy... */ }
   *   }
   * }
   */
  def apply[T, R](threads: Int, queueLength: Int)(factory: => Worker[T, R]): Workers[T, R] = {
    new Workers[T, R](threads, queueLength, factory)
  }

  /**
   * Convenience 'constructor'. Allows very concise syntax:
   * val workers = ResourceWorkers(loadFactor, queueDepth) {
   *   new Worker[Foo] {
   *     def init() = { /* init... */ }
   *     def process(value: T) = { /* work... */ }
   *     def destroy() = { /* destroy... */ }
   *   }
   * }
   */
  def apply[T, R](loadFactor: Double, queueDepth: Double)(factory: => Worker[T, R]): Workers[T, R] = {
    apply((defaultThreads * loadFactor).toInt, (defaultThreads * loadFactor * queueDepth).toInt)(factory)
  }

  /**
   * Convenience 'constructor'. Allows very concise syntax:
   * val workers = ResourceWorkers {
   *   new Worker[Foo] {
   *     def init() = { /* init... */ }
   *     def process(value: T) = { /* work... */ }
   *     def destroy() = { /* destroy... */ }
   *   }
   * }
   */
  def apply[T, R](factory: => Worker[T, R]): Workers[T, R] = {
    apply(defaultThreads, defaultThreads)(factory)
  }
}
