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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

class PipelineConf(spark: SparkSession) {
  private val sqlConf: SQLConf = spark.sessionState.conf

  val streamStatePollingInterval = sqlConf.getConf(SQLConf.PIPELINES_STREAM_STATE_POLLING_INTERVAL)

  val watchdogMinRetryTimeInSeconds = {
    val value = sqlConf.getConf(SQLConf.PIPELINES_WATCHDOG_MIN_RETRY_TIME_IN_SECONDS)
    if (value <= 0L) {
      throw new IllegalArgumentException(
        "Watchdog minimum retry time must be at least 1 second."
      )
    }
    value
  }

  val watchdogMaxRetryTimeInSeconds = {
    val value = sqlConf.getConf(SQLConf.PIPELINES_WATCHDOG_MAX_RETRY_TIME_IN_SECONDS)
    if (value < watchdogMinRetryTimeInSeconds) {
      throw new IllegalArgumentException(
        "Watchdog maximum retry time must be greater than or equal to the watchdog minimum " +
        "retry time."
      )
    }
    value
  }

  val maxConcurrentFlows = sqlConf.getConf(SQLConf.PIPELINES_MAX_CONCURRENT_FLOWS)

  val timeoutMsForTerminationJoinAndLock = {
    val value = sqlConf.getConf(SQLConf.PIPELINES_TIMEOUT_MS_FOR_TERMINATION_JOIN_AND_LOCK)
    if (value <= 0L) {
      throw new IllegalArgumentException(
        "Timeout for lock must be at least 1 millisecond."
      )
    }
    value
  }

  val maxFlowRetryAttempts = sqlConf.getConf(SQLConf.PIPELINES_MAX_FLOW_RETRY_ATTEMPTS)
}
