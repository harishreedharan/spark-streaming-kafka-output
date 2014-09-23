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

import java.io.File
import java.util.Properties

import kafka.producer.KeyedMessage
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka.util.TestUtil
import org.junit.{Before, Test, Assert}

import scala.collection.mutable

class TestKafkaOutputDStream {
  private val testUtil: TestUtil = TestUtil.getInstance
  // Name of the framework for Spark context
  def framework = this.getClass.getSimpleName

  // Master for Spark context
  def master = "local[2]"
  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(framework)
//  conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
  val ssc = new StreamingContext(conf, Duration.apply(2000))

  @Before def setup() {
    testUtil.prepare()
    val topics: java.util.List[String] = new java.util.ArrayList[String](3)
    topics.add("default")
    topics.add("static")
    topics.add("custom")
    testUtil.initTopicList(topics)
  }

  @Test
  def testKafkaDStream(): Unit = {
    val q: mutable.Queue[String] = new mutable.Queue[String]()
    q.enqueue("a", "b", "c")
    val tobe = mutable.Queue(ssc.sc.makeRDD(q.toSeq))
    val instream = ssc.queueStream(tobe)
    val producerConf = new Properties()
    producerConf.put("serializer.class", "kafka.serializer.DefaultEncoder")
    producerConf.put("key.serializer.class", "kafka.serializer.StringEncoder")
    producerConf.put("metadata.broker.list", testUtil.getKafkaServerUrl)
    producerConf.put("request.required.acks", "1")
    KafkaWriter.writeDStreamToKafka(instream, producerConf, (x: String) => new KeyedMessage[String,
      Array[Byte]]("default", null,x.getBytes))
      ssc.start()

    Thread.sleep(10000)
    val fetchedMsg: String = new String(
      testUtil.getNextMessageFromConsumer("default").message.asInstanceOf[Array[Byte]])
    Assert.assertNotNull(fetchedMsg)

  }
}
