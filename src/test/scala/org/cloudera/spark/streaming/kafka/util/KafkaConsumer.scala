/**
Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
  */
package org.apache.spark.streaming.kafka.util

import kafka.consumer.ConsumerConfig
import kafka.consumer.ConsumerIterator
import kafka.consumer.ConsumerTimeoutException
import kafka.consumer.KafkaStream
import kafka.message.MessageAndMetadata

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.HashMap
import java.util.List
import java.util.Map
import java.util.Properties

import scala.collection.JavaConversions._

/**
 * A Kafka Consumer implementation. This uses the current thread to fetch the
 * next message from the queue and doesn't use a multi threaded implementation.
 * So this implements a synchronous blocking call.
 * To avoid infinite waiting, a timeout is implemented to wait only for
 * 10 seconds before concluding that the message will not be available.
 */
object KafkaConsumer {
  private def createConsumerConfig(zkUrl: String, groupId: String): ConsumerConfig = {
    val props: Properties = new Properties
    props.put("zookeeper.connect", zkUrl)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "smallest")
    props.put("consumer.timeout.ms", "1000")
    new ConsumerConfig(props)
  }

}

class KafkaConsumer (val consumer: kafka.consumer.ConsumerConnector = kafka.consumer.Consumer.create(
  KafkaConsumer.createConsumerConfig(TestUtil.getInstance
      .getZkUrl, "group_1"))) {

  private[util] var consumerMap: Map[String, scala.List[KafkaStream[Array[Byte],
    Array[Byte]]]] = null

  private final val logger: Logger = LoggerFactory.getLogger(classOf[KafkaConsumer])

  def initTopicList(topics: List[String]) {
    val topicCountMap: Map[String, Int] = new HashMap[String, Int]
    for (topic <- topics) {
      topicCountMap.put(topic, new Integer(1))
    }
    consumerMap = consumer.createMessageStreams(topicCountMap)
  }

  def getNextMessage(topic: String): MessageAndMetadata[_, _] = {
    val streams: scala.List[KafkaStream[Array[Byte], Array[Byte]]] = consumerMap.get(topic)
    val stream: KafkaStream[Array[Byte], Array[Byte]] = streams.get(0)
    val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()
    try {
      if (it.hasNext()) {
        it.next()
      }
      else {
        null
      }
    }
    catch {
      case e: ConsumerTimeoutException => {
        logger.error("0 messages available to fetch for the topic " + topic)
        null
      }
    }
  }

  def shutdown(): Unit = {
    consumer.shutdown()
  }
}

