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

import kafka.server.KafkaConfig
import kafka.server.KafkaServerStartable
import java.util.Properties


/**
 * A local Kafka server for running unit tests.
 * Reference: https://gist.github.com/fjavieralba/7930018/
 */
class KafkaLocal(kafkaProperties: Properties) {
  private val kafkaConfig: KafkaConfig = new KafkaConfig(kafkaProperties)
  private val kafka = new KafkaServerStartable(kafkaConfig)

  def start() {
    kafka.startup()
  }

  def stop() {
    kafka.shutdown()
  }
}