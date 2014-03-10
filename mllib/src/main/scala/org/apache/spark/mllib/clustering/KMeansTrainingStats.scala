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

package org.apache.spark.mllib.clustering

import scala.collection.mutable.ArrayBuffer

class KMeansTrainingStats(val numIterations: Array[Int],
    val initialCosts: Array[Double],
    val finalCosts: Array[Double],
    val pointsSelectedPerInitIter: Array[ArrayBuffer[Int]]) extends Serializable {

  var initCentersTime: Long = 0
  var lloydsTime: Long = 0

  def this(numRuns: Int) {
    this(new Array[Int](numRuns), new Array[Double](numRuns),
      new Array[Double](numRuns),
      Array.tabulate(numRuns)(r => new ArrayBuffer[Int]()))
  }
}
