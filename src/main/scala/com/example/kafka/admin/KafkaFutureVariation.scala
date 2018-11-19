package com.example.kafka.admin

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.KafkaFuture

/** [[KafkaFuture]] wrapper instance. */
object KafkaFutureVariation extends LazyLogging {
  val dummyKafkaCompletedFuture: KafkaFuture[Void] = KafkaFuture.completedFuture(null)

  def thenApply[ResultT, ReturnT](future: KafkaFuture[ResultT])(fn: ResultT => ReturnT): KafkaFuture[ReturnT] = {
    logger.debug("compose kafka then apply.")

    future.thenApply(this.createKafkaFutureBaseFunction(fn))
  }

  def whenComplete[ResultT](future: KafkaFuture[ResultT])(fn: (ResultT, Throwable) => Unit): KafkaFuture[ResultT] = {
    logger.debug("compose kafka when complete.")

    future.whenComplete(this.createKafkaFutureBiConsumer(fn))
  }

  private def createKafkaFutureBaseFunction[ResultT, ReturnT](fn: ResultT => ReturnT): KafkaFuture.BaseFunction[ResultT, ReturnT] = {
    logger.debug("create kafka future base function.")

    new KafkaFuture.BaseFunction[ResultT, ReturnT] {
      override def apply(result: ResultT): ReturnT = fn(result)
    }
  }

  private def createKafkaFutureBiConsumer[ResultT](fn: (ResultT, Throwable) => Unit): KafkaFuture.BiConsumer[ResultT, Throwable] = {
    logger.debug("create kafka future bi consumer.")

    new KafkaFuture.BiConsumer[ResultT, Throwable] {
      override def accept(result: ResultT, exception: Throwable): Unit = fn(result, exception)
    }
  }
}
