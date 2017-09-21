package org.dbpedia.quad.processing
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.dbpedia.quad.processing.Workers.defaultThreads

import scala.collection.generic.CanBuildFrom
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
  * Created by chile on 12.06.17.
  */
class PromisedWork[T, R](factory: => Worker[T, R], val availThreads: Int = defaultThreads , val queueLength: Int = defaultThreads)
{
  def getFactory: Worker[T, R] = factory

  private val workers = for(i <- 0 until availThreads) yield i -> factory
  private val c = new AtomicInteger(0)

  def promise(future: Future[R]): Promise[R] ={
    val promise = Promise[R]()
    promise.completeWith(future)
    promise
  }

  def promise(tryy: Try[R]): Promise[R] ={
    this.promise(Future.fromTry(tryy))
  }

  def work(value: T): Promise[R] ={
    this.promise {
      val worker = workers.toMap.get(c.getAndIncrement() % availThreads).get
      worker.process(value)(PromisedWork.executionContext)
    }
  }

  def work(values: Seq[T]): Seq[Promise[R]] ={
    for(value <- values) yield
      work(value)
  }
}

object PromisedWork{

  val defaultThreads: Int = Runtime.getRuntime.availableProcessors

  private val executor = new WorkerExecutor(defaultThreads, defaultThreads, 10000, TimeUnit.MILLISECONDS, 10)
  private implicit val executionContext = ExecutionContext.fromExecutor(executor)

  def shutdownExecutor(): Unit = executor.shutdown()

  def isCompletedSuccessfully(promise: Promise[_]): Boolean ={
    isCompletedSuccessfully(promise.future)
  }

  def isCompletedSuccessfully(future: Future[_]): Boolean ={
      future.value match{
        case Some(v) => v match {
          case Success(s) => true
          case Failure(f) => false
        }
        case None => false
      }
  }

  private def lift[T](futures: Traversable[Future[T]]) ={
    futures.map(Await.result(_, Duration.Inf))
    futures
  }

  def waitPromises[T](promises: Seq[Promise[T]]): Future[Traversable[T]] =
    waitFutures(promises.map(z => z.future))

  def waitFutures[T](futures: Seq[Future[T]])(implicit cbf: CanBuildFrom[Traversable[Future[T]], T, Traversable[T]]): Future[Traversable[T]] =
    Future.sequence(lift(futures))(cbf, executionContext) // having neutralized exception completions through the lifting, .sequence can now be used

  def workInParallel[T, R](workers: Traversable[PromisedWork[T, R]], args: Seq[T]): Traversable[Promise[R]] = {
    try {
      for (worker <- workers;
           arg <- args) yield
          worker.work(arg)
    }
    finally {
    }
  }

  /**
    * Convenience 'constructor'. Allows very concise syntax:
    * val workers = SimpleWorkers(threads, queueLength) { foo: Foo =>
    *   // do something with foo...
    * }
    */

  def apply[T, R](threads: Int, queueLength: Int)(proc: T => R): PromisedWork[T, R] = {
    new PromisedWork[T, R](new Worker[T, R]() {
      private var state : WorkerState.Value = WorkerState.declared
      def init() = {state = WorkerState.initialized}
      def process(value: T)(implicit ec: ExecutionContext) = Future.apply(proc(value))(ec)
      def destroy() = {state = WorkerState.destroyed}
      def getState: org.dbpedia.quad.processing.WorkerState.Value = state
    }, threads, queueLength)
  }

  /**
    * Convenience 'constructor'. Allows very concise syntax:
    * val workers = SimpleWorkers(threads, queueLength) { foo: Foo =>
    *   // do something with foo...
    * }
    */

  def byFuture[T, R](threads: Int, queueLength: Int)(proc: T => Future[R]): PromisedWork[T, R] = {
    new PromisedWork[T, R](new Worker[T, R]() {
      private var state : WorkerState.Value = WorkerState.declared
      def init() = {state = WorkerState.initialized}
      def process(value: T)(implicit ec: ExecutionContext) = proc(value)
      def destroy() = {state = WorkerState.destroyed}
      def getState: org.dbpedia.quad.processing.WorkerState.Value = state
    }, threads, queueLength)
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