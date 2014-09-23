/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.kafka

import java.util.Properties

import kafka.producer.{ProducerConfig, KeyedMessage, Producer}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

object KafkaProducerWrapper {

  private var producerOpt: Option[Any] = None

  def getProducer[K,V](props: Properties): Producer[K,V] = {
    this.synchronized {
      producerOpt match {
        case Some(producerInstance) =>
          producerInstance.asInstanceOf[Producer[K,V]]
        case None =>
          val producerInstance = new Producer[K,V](new ProducerConfig(props))
          producerOpt = Option(producerInstance.asInstanceOf[Any])
          producerInstance
      }
    }
  }
}
object KafkaWriter {

  def writeDStreamToKafka [T: ClassTag, K, V](dstream: DStream[T], producerConfig: Properties,
    serializerFunc: T => KeyedMessage[K,V]): Unit = {

    println("Generating job")
    val func = (rdd: RDD[T]) => {
      rdd.foreachPartition(events => {
        KafkaProducerWrapper.getProducer(producerConfig)
          .send(events.map(serializerFunc).toArray: _*)
        println("Events sent to kafka")
      })
    }
    dstream.foreachRDD(func)

  }
}
