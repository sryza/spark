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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.pipelines.utils.{PipelineTest, TestGraphRegistrationContext}

class GraphOperationsSuite extends PipelineTest {
  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sessionState.catalog.reset()
  }

  // Helper functions to make the tests less verbose

  private def id(name: String): TableIdentifier =
    fullyQualifiedIdentifier(name)

  private def id(name: String, isView: Boolean): TableIdentifier =
    fullyQualifiedIdentifier(name, isView = isView)

  /**
   *             d -> e
   *             v
   * a -> (b) -> c     f -> g
   */
  lazy val graph: DataflowGraph = {
    val mem = MemoryStream(Encoders.INT, spark.sqlContext)
    val pipelineDef = new TestGraphRegistrationContext(spark) {
      registerTable("a")
      registerFlow("a", "`a-1`", query = dfFlowFunc(mem.toDF()))
      registerView("b", query = readFlowFunc("a"))
      registerTable("c")
      registerFlow("c", "c", query = readFlowFunc("b"))
      registerFlow("c", "`c-2`", query = readFlowFunc("d"))
      registerTable("d", query = Option(dfFlowFunc(mem.toDF())))
      registerTable("e")
      registerFlow("e", "`e-1`", query = readFlowFunc("d"))
      registerTable("f", query = Option(dfFlowFunc(mem.toDF())))
      registerTable("g", query = Option(readFlowFunc("f")))
    }

    pipelineDef.resolveToDataflowGraph()
  }

  test("graph is constructed correctly") {
    assert(graph.flowNodes(id("a-1")).inputs == Set())
    assert(graph.flowNodes(id("a-1")).output == id("a"))

    assert(graph.flowNodes(id("b", isView = true)).inputs == Set(id("a")))
    assert(graph.flowNodes(id("b", isView = true)).output == id("b", isView = true))

    assert(graph.flowNodes(id("c")).inputs == Set(id("b", isView = true)))
    assert(graph.flowNodes(id("c")).output == id("c"))
    assert(graph.flowNodes(id("c-2")).inputs == Set(id("d")))
    assert(graph.flowNodes(id("c-2")).output == id("c"))

    assert(graph.flowNodes(id("d")).inputs == Set())
    assert(graph.flowNodes(id("d")).output == id("d"))

    assert(graph.flowNodes(id("e-1")).inputs == Set(id("d")))
    assert(graph.flowNodes(id("e-1")).output == id("e"))

    assert(graph.flowNodes(id("f")).inputs == Set())
    assert(graph.flowNodes(id("f")).output == id("f"))

    assert(graph.flowNodes(id("g")).inputs == Set(id("f")))
    assert(graph.flowNodes(id("g")).output == id("g"))
  }

  test("upstream dependencies") {
    assert(graph.upstreamDatasets(id("a")) == Set())
    assert(graph.upstreamDatasets(id("b", isView = true)) == Set("a").map(id))
    assert(graph.upstreamDatasets(id("c")) == Set("a", "d").map(id) ++ Set(id("b", isView = true)))
    assert(graph.upstreamDatasets(id("d")) == Set())
    assert(graph.upstreamDatasets(id("e")) == Set("d").map(id))
    assert(graph.upstreamDatasets(id("f")) == Set())
    assert(graph.upstreamDatasets(id("g")) == Set("f").map(id))

    assert(graph.upstreamFlows(id("a-1")) == Set())
    assert(graph.upstreamFlows(id("b", isView = true)) == Set("a-1").map(id))
    assert(graph.upstreamFlows(id("c")) == Set("a-1").map(id) ++ Set(id("b", isView = true)))
    assert(graph.upstreamFlows(id("c-2")) == Set("d").map(id))
    assert(graph.upstreamFlows(id("d")) == Set())
    assert(graph.upstreamFlows(id("e-1")) == Set("d").map(id))
    assert(graph.upstreamFlows(id("f")) == Set())
    assert(graph.upstreamFlows(id("g")) == Set("f").map(id))

    val test1 = graph.upstreamDatasets(Seq("a", "d", "f").map(id))
    assert(test1 == Map.empty, "source nodes have no upstream datasets")

    val test2 = graph.upstreamDatasets(Seq("e", "g").map(id))
    assert(
      test2 == Map(id("d") -> Set(id("e")), id("f") -> Set(id("g"))),
      "incorrect upstream datasets"
    )

    val test3 = graph.upstreamDatasets(Seq("c").map(id))
    assert(
      test3 == Map(
        id("a") -> Set(id("c")),
        id("b", isView = true) -> Set(id("c")),
        id("d") -> Set(id("c"))
      ),
      "incorrect upstream datasets"
    )
  }

  test("downstream dependencies") {
    assert(graph.downstreamFlows(id("a-1")) == Set("c").map(id) ++ Set(id("b", isView = true)))
    assert(graph.downstreamFlows(id("b", isView = true)) == Set("c").map(id))
    assert(graph.downstreamFlows(id("c")) == Set())
    assert(graph.downstreamFlows(id("c-2")) == Set())
    assert(graph.downstreamFlows(id("d")) == Set("c-2", "e-1").map(id))
    assert(graph.downstreamFlows(id("e-1")) == Set())
    assert(graph.downstreamFlows(id("f")) == Set("g").map(id))
    assert(graph.downstreamFlows(id("g")) == Set())
  }
}
