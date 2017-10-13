package org.dbpedia.quad.processing

import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}


/**
  * Created by chile on 12.06.17.
  */
class WorkerExecutor(corePoolSize: Int,
                     maximumPoolSize: Int,
                     keepAliveTime: Long,
                     unit: TimeUnit,
                     queueCapacity: Int)
  extends ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, new ArrayBlockingQueue[Runnable](queueCapacity) {
    override def offer(e: Runnable): Boolean = {
      try {
        // if queue is full blocks until a task
        // is completed and hence no future tasks are submitted.
        put(e)
      } catch {
        case t: InterruptedException => Thread.currentThread().interrupt()
      }
        true
    }
  }) {
}
