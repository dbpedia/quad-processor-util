package org.dbpedia.quad.processing

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by chile on 11.06.17.
  */
trait Worker[T, R] {
  def init(): Unit
  def process(value: T)(implicit ec: ExecutionContext): Future[R]
  def destroy(): Unit
  def getState: WorkerState.Value
}
