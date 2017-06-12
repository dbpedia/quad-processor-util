package org.dbpedia.quad.processing

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
  * Created by chile on 12.06.17.
  */
class PromisedWork[T, R](availThreads: Int, queueLength: Int, factory: => Worker[T, R]){
  private implicit val executionContext = ExecutionContext.fromExecutor(new WorkerExecutor(
    availThreads, availThreads, Long.MaxValue, TimeUnit.NANOSECONDS, queueLength
  ))

  private val workers = for(i <- 0 until availThreads) yield (i -> factory)
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
      val worker = workers.toMap.get(c.getAndIncrement()%availThreads).get
      worker.process(value)(executionContext)
    }
  }
}

object PromisedWork{

  private def lift[T](futures: Traversable[Future[T]]) ={
    futures.map(Await.result(_, Duration.Inf))
    futures
  }

  def waitAll[T](futures: Traversable[Future[T]]) =
    Future.sequence(lift(futures)) // having neutralized exception completions through the lifting, .sequence can now be used

  def workInParallel[T, R](workers: Traversable[PromisedWork[T, R]], args: Seq[T]): Traversable[Promise[R]] = {
    try {
      for (worker <- workers;
           arg <- args) yield
          worker.work(arg)
    }
    finally {
    }
  }
}