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

/**
 * A simple object that can write to a Kafka topic.
 */
object KafkaWriter {
  /**
   * To write data from a DStream to Kafka, call this function after creating the DStream. Once
   * the DStream is passed into this function, all data coming from the DStream is written out to
   * Kafka. The properties instance takes the configuration required to connect to the Kafka
   * brokers in the standard Kafka format. The serializerFunc is a function that converts each
   * element of the RDD to a Kafka [[KeyedMessage]]. This closure should be serializable - so it
   * should use only instances of Serializables.
   * Here is an example: <p>
   * `&nbsp;&nbsp;val instream = ssc.queueStream(toBe)` <p>
   * `&nbsp;&nbsp;val producerConf = new Properties()`<p>
   * `&nbsp;&nbsp;producerConf.put("serializer.class", "kafka.serializer.DefaultEncoder")`<p>
   * `&nbsp;&nbsp;producerConf.put("key.serializer.class", "kafka.serializer.StringEncoder")`<p>
   * `&nbsp;&nbsp;producerConf.put("metadata.broker.list", testUtil.getKafkaServerUrl)`<p>
   * `&nbsp;&nbsp;producerConf.put("request.required.acks", "1")`<p>
   * `&nbsp;&nbsp;KafkaWriter.writeDStreamToKafka(instream, producerConf,`
   * `(x: String) => new KeyedMessage[String,String]("default", null,x))`<p>
   * `ssc.start()`<p>
   * @param dstream - The [[DStream]] to be written to Kafka
   * @param producerConfig - The configuration to be used to connect to the Kafka broker(s).
   * @param serializerFunc - The function to serialize the RDD to [[KeyedMessage]]s
   * @tparam T - The type of elements in the RDD
   * @tparam K - The type of the key of the [[KeyedMessage]]
   * @tparam V - The type of the actual message to be sent to Kafka
   *
   *
   *
   */
  def writeDStreamToKafka[T: ClassTag, K, V](dstream: DStream[T], producerConfig: Properties,
    serializerFunc: T => KeyedMessage[K, V]): Unit = {

    // Broadcast the producer to avoid sending it every time.
    val producer = dstream.ssc.sc.broadcast(new Producer[K, V](new ProducerConfig(producerConfig)))
    val func = (rdd: RDD[T]) => {
      rdd.foreachPartition(events => {
        // Get the producer from that local executor and write!
        producer.value.send(events.map(serializerFunc).toArray: _*)
      })
    }
    dstream.foreachRDD(func)
  }

}
