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

import org.apache.zookeeper.server.ServerConfig
import org.apache.zookeeper.server.ZooKeeperServerMain
import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.Properties

/**
 * A local Zookeeper server for running unit tests.
 * Reference: https://gist.github.com/fjavieralba/7930018/
 */
class ZooKeeperLocal(zkProperties: Properties) {
  private val quorumConfiguration: QuorumPeerConfig = new QuorumPeerConfig
  private final val logger: Logger = LoggerFactory.getLogger(classOf[ZooKeeperLocal])
  private val zooKeeperServer: ZooKeeperServerMain = new ZooKeeperServerMain
  private val configuration: ServerConfig = new ServerConfig
  try {
    quorumConfiguration.parseProperties(zkProperties)
  }
  catch {
    case e: Exception =>
      throw new RuntimeException(e)
  }
  configuration.readFrom(quorumConfiguration)
  new Thread {
    override def run() {
      try {
        zooKeeperServer.runFromConfig(configuration)
      }
      catch {
        case e: IOException =>
          logger.error("Zookeeper startup failed.", e)
      }
    }
  }.start()
}

