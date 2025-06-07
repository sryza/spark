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

package org.apache.spark.sql.connect.pipelines

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{DatasetType, Expression, PipelineCommand, Relation, UnresolvedTableValuedFunction}
import org.apache.spark.connect.proto.PipelineCommand.{DefineDataset, DefineFlow}
import org.apache.spark.internal.Logging

class SparkDeclarativePipelinesServerSuite
    extends SparkDeclarativePipelinesServerTest
    with Logging {
  test("CreateDataflowGraph request creates a new graph") {
    withRawBlockingStub { implicit stub =>
      assert(Option(createDataflowGraph(stub)).isDefined)
    }
  }

  test("Cross dependency between SQL dataset and non-SQL dataset is valid and can be registered") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      sendPlan(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setDefineDataset(
              DefineDataset
                .newBuilder()
                .setDataflowGraphId(graphId)
                .setDatasetName("mv")
                .setDatasetType(DatasetType.MATERIALIZED_VIEW)
            )
            .build()
        )
      )

      sendPlan(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setDefineFlow(
              DefineFlow
                .newBuilder()
                .setDataflowGraphId(graphId)
                .setFlowName("mv")
                .setTargetDatasetName("mv")
                .setPlan(
                  Relation
                    .newBuilder()
                    .setUnresolvedTableValuedFunction(
                      UnresolvedTableValuedFunction
                        .newBuilder()
                        .setFunctionName("range")
                        .addArguments(
                          Expression
                            .newBuilder()
                            .setLiteral(
                              Expression.Literal.newBuilder().setInteger(5).build()
                            )
                            .build()
                        )
                        .build()
                    )
                    .build()
                )
            )
            .build()
        )
      )
      registerGraphElementsFromSql(
        graphId = graphId,
        sql = """
                |CREATE MATERIALIZED VIEW mv2 AS SELECT 2;
                |CREATE FLOW f AS INSERT INTO mv2 BY NAME SELECT * FROM mv
                |""".stripMargin
      )

      val definition =
        DataflowGraphRegistry
          .getDataflowGraphOrThrow(graphId)

      val graph = definition.toDataflowGraph.resolve()

      assert(graph.flows.size == 3)
      assert(graph.tables.size == 2)
      assert(graph.views.isEmpty)

      val mvFlow =
        graph.resolvedFlows.filter(_.identifier.unquotedString == "spark_catalog.default.mv").head
      assert(mvFlow.inputs == Set())
      assert(mvFlow.destinationIdentifier.unquotedString == "spark_catalog.default.mv")

      val mv2Flow =
        graph.resolvedFlows.filter(_.identifier.unquotedString == "spark_catalog.default.mv2").head
      assert(mv2Flow.inputs == Set())
      assert(mv2Flow.destinationIdentifier.unquotedString == "spark_catalog.default.mv2")

      // flow defined in SQL that connects the non SQL dataset mv to the SQL dataset mv2 should
      // work.
      val namedFlow =
        graph.resolvedFlows.filter(_.identifier.unquotedString == "spark_catalog.default.f").head
      assert(namedFlow.inputs.map(_.unquotedString) == Set("spark_catalog.default.mv"))
      assert(namedFlow.destinationIdentifier.unquotedString == "spark_catalog.default.mv2")
    }
  }

  test("simple graph resolution test") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = new TestPipelineDefinition(graphId) {
        createTable(
          name = "tableA",
          datasetType = DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(5)")
        )
        createView(name = "viewB", sql = "SELECT * FROM tableA")
        createTable(
          name = "tableC",
          datasetType = DatasetType.TABLE,
          sql = Some("SELECT * FROM tableA, viewB")
        )
      }

      val definition =
        DataflowGraphRegistry
          .getDataflowGraphOrThrow(graphId)

      registerPipelineDatasets(pipeline)
      val graph = definition.toDataflowGraph
        .resolve()

      assert(graph.flows.size == 3)
      assert(graph.tables.size == 2)
      assert(graph.views.size == 1)

      val tableCFlow =
        graph.resolvedFlows
          .filter(_.identifier.unquotedString == "spark_catalog.default.tableC")
          .head
      assert(
        tableCFlow.inputs.map(_.unquotedString) == Set(
          "viewB",
          "spark_catalog.default.tableA"
        )
      )

      val viewBFlow =
        graph.resolvedFlows.filter(_.identifier.unquotedString == "viewB").head
      assert(viewBFlow.inputs.map(_.unquotedString) == Set("spark_catalog.default.tableA"))

      val tableAFlow =
        graph.resolvedFlows
          .filter(_.identifier.unquotedString == "spark_catalog.default.tableA")
          .head
      assert(tableAFlow.inputs == Set())
    }
  }

  test("execute pipeline end-to-end test") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph(stub)

      val pipeline = new TestPipelineDefinition(graphId) {
        createTable(
          name = "tableA",
          datasetType = DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(5)")
        )
        createTable(
          name = "tableB",
          datasetType = DatasetType.TABLE,
          sql = Some("SELECT * FROM STREAM tableA")
        )
        createTable(
          name = "tableC",
          datasetType = DatasetType.TABLE,
          sql = Some("SELECT * FROM tableB")
        )
      }

      registerPipelineDatasets(pipeline)
      startPipelineAndWaitForCompletion(graphId)
      // TODO: Remove the eventually block once startPipelineAndWaitForCompletion actually blocks
      // on pipeline completion.
      eventually(timeout(15.seconds)) {
        // Check that each table has the correct data.
        assert(spark.table("spark_catalog.default.tableA").count() == 5)
        assert(spark.table("spark_catalog.default.tableB").count() == 5)
        assert(spark.table("spark_catalog.default.tableC").count() == 5)
      }
    }
  }

  test("get resolved dataflow graph") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = new TestPipelineDefinition(graphId) {
        createTable(
          name = "tableA",
          datasetType = DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(5)")
        )
        createView(name = "viewB", sql = "SELECT * FROM tableA")
        createTable(
          name = "tableC",
          datasetType = DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM tableA, viewB")
        )
      }

      registerPipelineDatasets(pipeline)

      // Get the resolved dataflow graph
      val response = sendPlan(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setGetResolvedDataflowGraph(
              PipelineCommand.GetResolvedDataflowGraph
                .newBuilder()
                .setDataflowGraphId(graphId)
                .build()
            )
            .build()
        )
      )

      // Verify the response contains the expected flow definitions and dataset definitions
      val result = response.getPipelineCommandResult.getGetResolvedDataflowGraphResult

      // Verify flow definitions
      val flowDefs = result.getFlowDefinitionsList.asScala
      assert(flowDefs.size == 3, "Expected 3 flow definitions")

      val tableCFlow = flowDefs
        .find(
          f =>
            f.getFlowId.getName == "tableC" &&
            f.getFlowId.getCatalogName == "spark_catalog" &&
            f.getFlowId.getNamespaceList.asScala.contains("default")
        )
        .get
      assert(tableCFlow.getTargetDatasetId.getName == "tableC")
      assert(tableCFlow.getTargetDatasetId.getCatalogName == "spark_catalog")
      assert(tableCFlow.getTargetDatasetId.getNamespaceList.asScala.contains("default"))

      val tableCInputs = tableCFlow.getInputDatasetIdsList.asScala
      assert(
        tableCInputs.exists(
          id =>
            id.getName == "tableA" &&
            id.getCatalogName == "spark_catalog" &&
            id.getNamespaceList.asScala.contains("default")
        )
      )
      assert(
        tableCInputs.exists(
          id =>
            id.getName == "viewB" &&
            !id.hasCatalogName &&
            id.getNamespaceCount == 0
        )
      )

      val viewBFlow = flowDefs
        .find(
          f =>
            f.getFlowId.getName == "viewB" &&
            !f.getFlowId.hasCatalogName &&
            f.getFlowId.getNamespaceCount == 0
        )
        .get
      assert(viewBFlow.getTargetDatasetId.getName == "viewB")
      assert(!viewBFlow.getTargetDatasetId.hasCatalogName)
      assert(viewBFlow.getTargetDatasetId.getNamespaceCount == 0)

      val viewBInputs = viewBFlow.getInputDatasetIdsList.asScala
      assert(
        viewBInputs.exists(
          id =>
            id.getName == "tableA" &&
            id.getCatalogName == "spark_catalog" &&
            id.getNamespaceList.asScala.contains("default")
        )
      )

      val tableAFlow = flowDefs
        .find(
          f =>
            f.getFlowId.getName == "tableA" &&
            f.getFlowId.getCatalogName == "spark_catalog" &&
            f.getFlowId.getNamespaceList.asScala.contains("default")
        )
        .get
      assert(tableAFlow.getTargetDatasetId.getName == "tableA")
      assert(tableAFlow.getTargetDatasetId.getCatalogName == "spark_catalog")
      assert(tableAFlow.getTargetDatasetId.getNamespaceList.asScala.contains("default"))
      assert(tableAFlow.getInputDatasetIdsCount == 0)

      // Verify dataset definitions
      val datasetDefs = result.getDatasetDefinitionsList.asScala
      assert(datasetDefs.size == 3, "Expected 3 dataset definitions")

      val tableADataset = datasetDefs
        .find(
          d =>
            d.getDatasetId.getName == "tableA" &&
            d.getDatasetId.getCatalogName == "spark_catalog" &&
            d.getDatasetId.getNamespaceList.asScala.contains("default")
        )
        .get
      assert(tableADataset.getDatasetType == DatasetType.MATERIALIZED_VIEW)

      val viewBDataset = datasetDefs
        .find(
          d =>
            d.getDatasetId.getName == "viewB" &&
            !d.getDatasetId.hasCatalogName &&
            d.getDatasetId.getNamespaceCount == 0
        )
        .get
      assert(viewBDataset.getDatasetType == DatasetType.TEMPORARY_VIEW)

      val tableCDataset = datasetDefs
        .find(
          d =>
            d.getDatasetId.getName == "tableC" &&
            d.getDatasetId.getCatalogName == "spark_catalog" &&
            d.getDatasetId.getNamespaceList.asScala.contains("default")
        )
        .get
      assert(tableCDataset.getDatasetType == DatasetType.MATERIALIZED_VIEW)
    }
  }

  test("get resolved dataflow graph with complex dependencies") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph

      // Create a more complex pipeline with multiple levels of dependencies
      val pipeline = new TestPipelineDefinition(graphId) {
        createTable(
          name = "source1",
          datasetType = DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(10)")
        )
        createTable(
          name = "source2",
          datasetType = DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(5, 15)")
        )
        createView(
          name = "intermediate1",
          sql = "SELECT * FROM source1 WHERE id % 2 = 0"
        )
        createView(
          name = "intermediate2",
          sql = "SELECT * FROM source2 WHERE id % 3 = 0"
        )
        createTable(
          name = "combined",
          datasetType = DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT a.id FROM intermediate1 a JOIN intermediate2 b ON a.id = b.id")
        )
        createTable(
          name = "final",
          datasetType = DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM combined UNION SELECT * FROM source1 WHERE id > 5")
        )
      }

      registerPipelineDatasets(pipeline)

      // Get the resolved dataflow graph
      val response = sendPlan(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setGetResolvedDataflowGraph(
              PipelineCommand.GetResolvedDataflowGraph
                .newBuilder()
                .setDataflowGraphId(graphId)
                .build()
            )
            .build()
        )
      )

      val result = response.getPipelineCommandResult.getGetResolvedDataflowGraphResult

      // Verify flow definitions
      val flowDefs = result.getFlowDefinitionsList.asScala
      assert(flowDefs.size == 6, "Expected 6 flow definitions")

      // Helper function to find a flow by name
      def findFlow(
          name: String,
          catalog: Option[String] = Some("spark_catalog"),
          namespace: Option[String] = Some("default")): proto.ResolvedFlowDefinition = {
        flowDefs.find { f =>
          f.getFlowId.getName == name &&
          catalog.forall(c => f.getFlowId.getCatalogName == c) &&
          namespace.forall(ns => f.getFlowId.getNamespaceList.asScala.contains(ns))
        }.get
      }

      // Helper function to check if an input dataset exists
      def hasInputDataset(
          inputs: mutable.Buffer[proto.DatasetIdentifier],
          name: String,
          catalog: Option[String] = None,
          namespace: Option[String] = None): Boolean = {
        inputs.exists { id =>
          id.getName == name &&
          catalog.forall(c => id.getCatalogName == c) &&
          namespace.forall(ns => id.getNamespaceList.asScala.contains(ns))
        }
      }

      val finalFlow = findFlow("final")
      val finalInputs = finalFlow.getInputDatasetIdsList.asScala
      assert(hasInputDataset(finalInputs, "combined", Some("spark_catalog"), Some("default")))
      assert(hasInputDataset(finalInputs, "source1", Some("spark_catalog"), Some("default")))

      val combinedFlow = findFlow("combined")
      val combinedInputs = combinedFlow.getInputDatasetIdsList.asScala
      assert(hasInputDataset(combinedInputs, "intermediate1", None, None))
      assert(hasInputDataset(combinedInputs, "intermediate2", None, None))

      val int1Flow = findFlow("intermediate1", None, None)
      val int1Inputs = int1Flow.getInputDatasetIdsList.asScala
      assert(hasInputDataset(int1Inputs, "source1", Some("spark_catalog"), Some("default")))

      val int2Flow = findFlow("intermediate2", None, None)
      val int2Inputs = int2Flow.getInputDatasetIdsList.asScala
      assert(hasInputDataset(int2Inputs, "source2", Some("spark_catalog"), Some("default")))

      val source1Flow = findFlow("source1")
      assert(source1Flow.getInputDatasetIdsCount == 0)

      val source2Flow = findFlow("source2")
      assert(source2Flow.getInputDatasetIdsCount == 0)

      // Verify dataset definitions
      val datasetDefs = result.getDatasetDefinitionsList.asScala
      assert(datasetDefs.size == 6, "Expected 6 dataset definitions")

      // Helper function to find a dataset by name
      def findDataset(
          name: String,
          catalog: Option[String] = Some("spark_catalog"),
          namespace: Option[String] = Some("default")): proto.DatasetDefinition = {
        datasetDefs.find { d =>
          d.getDatasetId.getName == name &&
          catalog.forall(c => d.getDatasetId.getCatalogName == c) &&
          namespace.forall(ns => d.getDatasetId.getNamespaceList.asScala.contains(ns))
        }.get
      }

      assert(findDataset("source1").getDatasetType == DatasetType.MATERIALIZED_VIEW)
      assert(findDataset("source2").getDatasetType == DatasetType.MATERIALIZED_VIEW)
      assert(findDataset("intermediate1", None, None).getDatasetType == DatasetType.TEMPORARY_VIEW)
      assert(findDataset("intermediate2", None, None).getDatasetType == DatasetType.TEMPORARY_VIEW)
      assert(findDataset("combined").getDatasetType == DatasetType.MATERIALIZED_VIEW)
      assert(findDataset("final").getDatasetType == DatasetType.MATERIALIZED_VIEW)
    }
  }

  // Create a test that creates streaming tables, materialized views, and temporary views.
  // At least one table should read from a temporary view. At least one temporary view should
  // be a streaming view.
  test("create streaming tables, materialized views, and temporary views") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph

      sql(s"CREATE SCHEMA IF NOT EXISTS spark_catalog.`curr`")
      sql(s"CREATE SCHEMA IF NOT EXISTS spark_catalog.`other`")

      val pipeline = new TestPipelineDefinition(graphId) {
        createTable(
          name = "curr.tableA",
          datasetType = proto.DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(5)")
        )
        createTable(
          name = "curr.tableB",
          datasetType = proto.DatasetType.TABLE,
          sql = Some("SELECT * FROM STREAM curr.tableA")
        )
        createView(
          name = "viewC",
          sql = "SELECT * FROM curr.tableB"
        )
        createTable(
          name = "other.tableD",
          datasetType = proto.DatasetType.TABLE,
          sql = Some("SELECT * FROM viewC")
        )
      }

      registerPipelineDatasets(pipeline)
      startPipelineAndWaitForCompletion(graphId)

      // Check that each table has the correct data.
      assert(spark.table("spark_catalog.curr.tableA").count() == 5)
      assert(spark.table("spark_catalog.curr.tableB").count() == 5)
      assert(spark.table("spark_catalog.other.tableD").count() == 5)
    }
  }
}
