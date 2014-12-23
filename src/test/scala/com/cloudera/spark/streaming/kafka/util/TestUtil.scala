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

import java.net.{BindException, InetAddress, UnknownHostException}
import java.util.{List, Properties, Random}

import kafka.message.MessageAndMetadata
import org.slf4j.{Logger, LoggerFactory}

/**
 * A utility class for starting/stopping Kafka Server.
 */
object TestUtil {

  private val instance: TestUtil = new TestUtil()

  def getInstance: TestUtil = {
    instance
  }
}

class TestUtil {
  init()
  private val randPortGen: Random = new Random(System.currentTimeMillis)
  private var kafkaServer: KafkaLocal = null
  private var kafkaConsumer: KafkaConsumer = null
  private var hostname: String = "localhost"
  private var kafkaLocalPort: Int = 0
  private var zkLocalPort: Int = 0
  private final val logger: Logger = LoggerFactory.getLogger(classOf[TestUtil])

  private def init() {
    try {
      hostname = InetAddress.getLocalHost.getHostName
    }
    catch {
      case e: UnknownHostException => {
        logger.warn("Error getting the value of localhost. " + "Proceeding with 'localhost'.", e)
      }
    }
  }

  private def startKafkaServer: Boolean = {
    val kafkaProperties: Properties = new Properties
    val zkProperties: Properties = new Properties
    try {
      zkProperties.load(classOf[Class[_]].getResourceAsStream("/zookeeper.properties"))
      var zookeeper: ZooKeeperLocal = null
      var portAssigned = false
      while (!portAssigned) {
        try {
          zkLocalPort = getNextPort
          zkProperties.setProperty("clientPort", Integer.toString(zkLocalPort))
          zookeeper = new ZooKeeperLocal(zkProperties)
          portAssigned = true
        }
        catch {
          case bindEx: BindException => {
          }
        }
      }
      logger.info("ZooKeeper instance is successfully started on port " + zkLocalPort)
      kafkaProperties.load(classOf[Class[_]].getResourceAsStream("/kafka-server.properties"))
      kafkaProperties.setProperty("zookeeper.connect", getZkUrl)
      var started = false
      while (!started) {
        kafkaLocalPort = getNextPort
        kafkaProperties.setProperty("port", Integer.toString(kafkaLocalPort))
        kafkaServer = new KafkaLocal(kafkaProperties)
        try {
          kafkaServer.start()
          started = true
        }
        catch {
          case bindEx: BindException => {
          }
        }
      }
      logger.info("Kafka Server is successfully started on port " + kafkaLocalPort)
      true
    }
    catch {
      case e: Exception => {
        logger.error("Error starting the Kafka Server.", e)
        false
      }
    }
  }

  private def getKafkaConsumer: KafkaConsumer = {
    this synchronized {
      if (kafkaConsumer == null) {
        kafkaConsumer = new KafkaConsumer
      }
    }
    kafkaConsumer
  }

  def initTopicList(topics: List[String]) {
    getKafkaConsumer.initTopicList(topics)
  }

  def getNextMessageFromConsumer(topic: String): MessageAndMetadata[_, _] = {
    getKafkaConsumer.getNextMessage(topic)
  }

  def prepare() {
    val startStatus: Boolean = startKafkaServer
    if (!startStatus) {
      throw new RuntimeException("Error starting the server!")
    }
    try {
      Thread.sleep(3 * 1000)
    }
    catch {
      case e: InterruptedException => {
      }
    }
    getKafkaConsumer
    logger.info("Completed the prepare phase.")
  }

  def tearDown() {
    logger.info("Shutting down the Kafka Consumer.")
    getKafkaConsumer.shutdown()
    try {
      Thread.sleep(3 * 1000)
    }
    catch {
      case e: InterruptedException => {
      }
    }
    logger.info("Shutting down the kafka Server.")
    kafkaServer.stop()
    logger.info("Completed the tearDown phase.")
  }

  private def getNextPort: Int = {
    randPortGen.nextInt(65535 - 49152) + 49152
  }

  def getZkUrl: String = {
    hostname + ":" + zkLocalPort
  }

  def getKafkaServerUrl: String = {
    hostname + ":" + kafkaLocalPort
  }
}

