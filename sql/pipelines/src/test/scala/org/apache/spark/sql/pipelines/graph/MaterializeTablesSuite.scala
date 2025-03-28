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

// import org.apache.spark.SparkThrowable
// import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.pipelines.graph.DatasetManager.TableMaterializationException
import org.apache.spark.sql.pipelines.utils.{BaseCoreExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils.exceptionString
// import org.apache.spark.sql.execution.streaming.MemoryStream
// import org.apache.spark.sql.functions.{col, lit}
// import org.apache.spark.util.Utils.exceptionString

/**
 * Local integration tests for materialization of [[Table]]s in a [[DataflowGraph]] to make sure
 * tables are written with the appropriate schemas.
 */
class MaterializeTablesSuite extends BaseCoreExecutionTest {

  import originalSpark.implicits._

  test("basic") {
    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerFlow(
          "a",
          "a",
          query = dfFlowFunc(Seq((1, 1), (2, 3)).toDF("x", "x2"))
        )
        registerTable(
          "a",
          specifiedSchema = Option(
            new StructType()
              .add("x", IntegerType, false, "comment1")
              .add("x2", IntegerType, true, "comment2")
          ),
          comment = Option("p-comment")
        )
      }.resolveToDataflowGraph()
    )

    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "a")
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val catalogTable = catalog.loadTable(identifier)

    assert(
      catalogTable.schema == new StructType()
        .add("x", IntegerType, false, "comment1")
        .add("x2", IntegerType, true, "comment2")
    )
    assert(catalogTable.properties().get(TableCatalog.PROP_COMMENT) == "p-comment")

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerFlow(
          "a",
          "a",
          query = dfFlowFunc(Seq((1, 1), (2, 3)).toDF("x", "x2"))
        )
        registerTable(
          "a",
          specifiedSchema = Option(
            new StructType()
              .add("x", IntegerType, false, "comment3")
              .add("x2", IntegerType, true, "comment4")
          ),
          comment = Option("p-comment")
        )
      }.resolveToDataflowGraph()
    )
    val catalogTable2 = catalog.loadTable(identifier)
    assert(
      catalogTable2.schema == new StructType()
        .add("x", IntegerType, false, "comment3")
        .add("x2", IntegerType, true, "comment4")
    )
    assert(catalogTable2.properties().get(TableCatalog.PROP_COMMENT) == "p-comment")

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerFlow(
          "a",
          "a",
          query = dfFlowFunc(Seq((1, 1), (2, 3)).toDF("x", "x2"))
        )
        registerTable(
          "a",
          specifiedSchema = Option(
            new StructType()
              .add("x", IntegerType, false)
              .add("x2", IntegerType, true)
          ),
          comment = Option("p-comment")
        )
      }.resolveToDataflowGraph()
    )

    val catalogTable3 = catalog.loadTable(identifier)
    assert(
      catalogTable3.schema == new StructType()
        .add("x", IntegerType, false, comment = null)
        .add("x2", IntegerType, true, comment = null)
    )
    assert(catalogTable3.properties().get(TableCatalog.PROP_COMMENT) == "p-comment")
  }

  test("multiple") {
    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerFlow(
          "t1",
          "t1",
          query = dfFlowFunc(Seq(1, 2, 3).toDF("x"))
        )
        registerFlow(
          "t2",
          "t2",
          query = dfFlowFunc(Seq("a", "b").toDF("y"))
        )
        registerTable("t1")
        registerTable("t2")
      }.resolveToDataflowGraph()
    )

    val identifier1 = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t1")
    val identifier2 = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t2")
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val catalogTable1 = catalog.loadTable(identifier1)
    val catalogTable2 = catalog.loadTable(identifier2)

    assert(catalogTable1.schema == new StructType().add("x", IntegerType))
    assert(catalogTable2.schema == new StructType().add("y", StringType))
  }

//  test("non materialized tables don't get materialized") {
//    val identifier1 = new TableIdentifier("t1", schemaInPipelineSpec)
//    val identifier2 = new TableIdentifier("t2", schemaInPipelineSpec)
//
//    materializeGraph(
//      new TestGraphRegistrationContext(
//        flows = Seq(
//         registerFlow(
//            identifier1,
//            identifier1,
//            query = dfFlowFunc(Seq(1, 2, 3).toDF("x")),
//          ),
//         registerFlow(
//            TableIdentifier("t2"),
//            TableIdentifier("t2"),
//            query = dfFlowFunc(Seq("a", "b").toDF("y"))
//          )
//        ),
//        tables = Seq(Table(TableIdentifier("t2"), explicitPath = Option(p + "/t2"))),
//        views = Seq(
//          TemporaryView(
//            identifier = identifier1,
//          )
//        )
//      )
//    )
//
//    assert(deltaLog1.snapshot.version < 0)
//    assert(deltaLog2.snapshot.version == 0)
//    assert(deltaLog2.snapshot.schema == new StructType().add("y", StringType))
//  }

  //  scalastyle:off

  // TableManager performs different validations for batch tables vs incremental tables when
  // materializing tables. Flows writing to a batch tables can have incompatible schemas with the
  // existing table since the table is being overwritten completely. This test ensures that
  // it is possible to do that.
  test("batch flow reading from incremental table") {
    class P1 extends TestGraphRegistrationContext(spark) {
      registerTable(
        "a",
        query = Option(dfFlowFunc(spark.readStream.format("rate").load()))
      )
      // Defines a column called timestamp as `int`.
      registerTable(
        "b",
        query = Option(sqlFlowFunc(spark, "SELECT value AS timestamp FROM a"))
      )
    }
    materializeGraph(new P1().resolveToDataflowGraph())

    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val b =
      catalog.loadTable(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "b"))
    assert(b.schema == new StructType().add("timestamp", LongType))

    class P2 extends TestGraphRegistrationContext(spark) {
      registerTable(
        "a",
        query = Option(dfFlowFunc(spark.readStream.format("rate").load()))
      )
      // Defines a column called timestamp as `timestamp`.
      registerTable(
        "b",
        query = Option(sqlFlowFunc(spark, "SELECT timestamp FROM a"))
      )
    }
    materializeGraph(new P2().resolveToDataflowGraph())
    val b2 =
      catalog.loadTable(Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "b"))
    assert(b2.schema == new StructType().add("timestamp", TimestampType))
  }

  test("schema matches existing table schema") {
    sql(s"CREATE TABLE ${TestGraphRegistrationContext.DEFAULT_DATABASE}.t2(x INT)")
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t2")
    val table = catalog.loadTable(identifier)
    assert(table.schema == new StructType().add("x", IntegerType))

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerFlow("t2", "t2", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
        registerTable("t2")
      }.resolveToDataflowGraph()
    )

    val table2 = catalog.loadTable(identifier)
    assert(table2.schema == new StructType().add("x", IntegerType))
  }
//
  test("invalid schema merge") {
    val streamInts = MemoryStream[Int]
    streamInts.addData(1, 2)

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerView("a", query = dfFlowFunc(streamInts.toDF()))
        registerTable("b", query = Option(sqlFlowFunc(spark, "SELECT value AS x FROM STREAM a")))
      }.resolveToDataflowGraph()
    )

    val streamStrings = MemoryStream[String]
    streamStrings.addData("a", "b")
    val graph2 = new TestGraphRegistrationContext(spark) {
      registerView("a", query = dfFlowFunc(streamStrings.toDF()))
      registerTable("b", query = Option(sqlFlowFunc(spark, "SELECT value AS x FROM STREAM a")))
    }.resolveToDataflowGraph()

    val ex = intercept[TableMaterializationException] {
      materializeGraph(graph2)
    }
    val cause = ex.cause
    val exStr = exceptionString(cause)
    assert(exStr.contains("Failed to merge incompatible data types"))
  }

  test("table materialized with specified schema, even if different from inferred") {
    sql(s"CREATE TABLE ${TestGraphRegistrationContext.DEFAULT_DATABASE}.t4(x INT)")
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t4")
    val table = catalog.loadTable(identifier)
    assert(table.schema == new StructType().add("x", IntegerType))

    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerFlow("t4", "t4", query = dfFlowFunc(Seq[Short](1, 2).toDF("x")))
        registerTable(
          "t4",
          specifiedSchema = Option(
            new StructType()
              .add("x", IntegerType, true, "this is column x")
              .add("z", LongType, true, "this is column z")
          )
        )
      }.resolveToDataflowGraph()
    )

    val table2 = catalog.loadTable(identifier)
    assert(
      table2.schema == new StructType()
        .add("x", IntegerType, true, "this is column x")
        .add("z", LongType, true, "this is column z")
    )
  }

  test("specified schema incompatible with existing table") {
    sql(s"CREATE TABLE ${TestGraphRegistrationContext.DEFAULT_DATABASE}.t6(x BOOLEAN)")
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t6")
    val table = catalog.loadTable(identifier)
    assert(table.schema == new StructType().add("x", BooleanType))

    val ex = intercept[TableMaterializationException] {
      materializeGraph(new TestGraphRegistrationContext(spark) {
        val source = MemoryStream[Int]
        source.addData(1, 2)
        registerTable(
          "t6",
          specifiedSchema = Option(new StructType().add("x", IntegerType)),
          query = Option(dfFlowFunc(source.toDF().select($"value" as "x")))
        )

      }.resolveToDataflowGraph())
    }
    val cause = ex.cause
    val exStr = exceptionString(cause)
    assert(exStr.contains("Failed to merge incompatible data types"))

    // Works fine for a complete table
    materializeGraph(new TestGraphRegistrationContext(spark) {
      registerTable(
        "t6",
        specifiedSchema = Option(new StructType().add("x", IntegerType)),
        query = Option(dfFlowFunc(Seq(1, 2).toDF("x")))
      )
    }.resolveToDataflowGraph())
    val table2 = catalog.loadTable(identifier)
    assert(table2.schema == new StructType().add("x", IntegerType))
  }

  test("partition columns with user schema") {
    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerTable(
          "a",
          query = Option(dfFlowFunc(Seq((1, 1), (2, 3)).toDF("x1", "x2"))),
          specifiedSchema = Option(
            new StructType()
              .add("x1", IntegerType)
              .add("x2", IntegerType)
          ),
          partitionCols = Option(Seq("x2"))
        )
      }.resolveToDataflowGraph()
    )
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "a")
    val table = catalog.loadTable(identifier)
    assert(
      table.schema ==
      new StructType().add("x1", IntegerType).add("x2", IntegerType)
    )
    assert(table.partitioning().toSeq == Seq(Expressions.identity("x2")))
  }

  test("specifying partition column with existing partitioned table") {
    sql(
      s"CREATE TABLE ${TestGraphRegistrationContext.DEFAULT_DATABASE}.t7(x BOOLEAN, y INT) PARTITIONED BY (x)"
    )
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = Identifier.of(Array(TestGraphRegistrationContext.DEFAULT_DATABASE), "t7")
    val table = catalog.loadTable(identifier)
    assert(
      table.schema.fields.toSet == new StructType()
        .add("x", BooleanType)
        .add("y", IntegerType)
        .fields
        .toSet
    )
    assert(table.partitioning().toSeq == Seq(Expressions.identity("x")))

    // Specify the same partition column.
    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerFlow(
          "t7",
          "t7",
          query = dfFlowFunc(Seq((true, 1), (false, 3)).toDF("x", "y"))
        )
        registerTable(
          "t7",
          partitionCols = Option(Seq("x"))
        )
      }.resolveToDataflowGraph()
    )

    val table2 = catalog.loadTable(identifier)
    assert(table2.schema == new StructType().add("y", IntegerType).add("x", BooleanType))
    assert(table2.partitioning().toSeq == Seq(Expressions.identity("x")))

    // Don't specify any partition column; use the one from the table.
    materializeGraph(
      new TestGraphRegistrationContext(spark) {
        registerFlow(
          "t7",
          "t7",
          query = dfFlowFunc(Seq((true, 1), (false, 3)).toDF("x", "y"))
        )
        registerTable("t7")
      }.resolveToDataflowGraph()
    )

    val table3 = catalog.loadTable(identifier)
    assert(table3.schema == new StructType().add("y", IntegerType).add("x", BooleanType))
    assert(table3.partitioning().toSeq == Seq(Expressions.identity("x")))
  }

//  test("specifying partition column different from existing partitioned table") {
//    sql(s"CREATE TABLE ${TestGraphRegistrationContext.DEFAULT_DATABASE}.t8(x BOOLEAN, y INT) PARTITIONED BY (x)")
//    Seq((true, 1), (false, 1)).toDF("x", "y").write.mode("append").format("delta").save(p)
//    assert(table.schema == new StructType().add("x", BooleanType).add("y", IntegerType))
//    assert(deltaLog.snapshot.dataSchema == new StructType().add("y", IntegerType))
//
//    // Specify a different partition column. Should throw.
//    val graph = DataflowGraph(
//      flows = Seq(
//       registerFlow(
//          TableIdentifier(p),
//          dummyPipeline,
//          TableIdentifier(p),
//          currentCatalog = catalogInPipelineSpec,
//          query = dfFlowFunc(Seq((true, 1), (false, 3)).toDF("x", "y"))
//        )
//      ),
//      tables = Seq(
//        Table(
//          identifier = TableIdentifier(p),
//          partitionCols = Option(Seq("y"))
//        )
//      ),
//      views = Seq.empty
//    )
//
//    val ex = intercept[TableMaterializationException] {
//      materializeGraph(graph)
//    }
//    assert(ex.cause.asInstanceOf[SparkThrowable].getErrorClass == "CANNOT_UPDATE_PARTITION_COLUMNS")
//    assert(table.schema == new StructType().add("x", BooleanType).add("y", IntegerType))
//    assert(deltaLog.snapshot.dataSchema == new StructType().add("y", IntegerType))
//  }
//
//  test("Table properties are set when table gets materialized") {
//
//    val graph =
//      materializeGraph(
//        DataflowGraph(
//          new TestGraphRegistrationContext(spark) {
//            registerTable("a")
//              .query(spark.readStream.format("rate").load())
//              .tableProperty("pipelines.autoOptimize.zordercols", "value")
//              .tableProperty("some.prop", "foo")
//              .tableProperty("delta.enableExpiredLogCleanup", "false")
//            registerTable("b")
//              .query(readStream("a"))
//              .tableProperty("pipelines.autoOptimize.zordercoLS", "value")
//              .tableProperty("some.prop", "foo")
//              .tableProperty("delta.enableExpiredLogCleanup", "false")
//          }
//        )
//      )
//    val deltaLogA = DeltaLog.forTable(spark, graph.tableByName(materializationName("a")).path)
//    val deltaLogB = DeltaLog.forTable(spark, graph.tableByName(materializationName("b")).path)
//
//    var expectedProps = Map(
//      "pipelines.autoOptimize.zOrderCols" -> "value",
//      "some.prop" -> "foo",
//      "delta.enableExpiredLogCleanup" -> "false",
//      "pipelines.pipelineId" -> updateContext.defaultOrigin.getPipelineId
//    )
//
//    val enableCDF = (updateContext.pipelineConf.cdfEnabled.value
//      || (updateContext.pipelineConf.enzymeModeEnabled(EnzymeRuntimeMode.Advanced)
//      && !updateContext.pipelineConf.cdfDisabledForEnzyme.value))
//    expectedProps = expectedProps + ("delta.enableChangeDataFeed" -> enableCDF.toString)
//
//    if (updateContext.pipelineConf.enzymeModeEnabled(EnzymeRuntimeMode.Advanced)) {
//      expectedProps = expectedProps + (InternalTablePropertyKey.ENZYME_MODE_KEY -> EnzymeRuntimeMode.Advanced.toString)
//    }
//
//    if (updateContext.isUCPipeline) {
//      expectedProps = expectedProps + (InternalTablePropertyKey.CATALOG_TYPE_KEY -> CatalogType.UNITY_CATALOG.toString)
//    }
//    assert(deltaLogA.snapshot.metadata.configuration == expectedProps)
//    assert(deltaLogB.snapshot.metadata.configuration == expectedProps)
//  }
//
//  test("Invalid table properties error during table materialization") {
//
//    // Invalid pipelines property
//    val graph1 = DataflowGraph(
//      new TestGraphRegistrationContext(spark) {
//        registerTable("a")
//          .query(Seq(1).toDF())
//          .tableProperty("pipelines.autoOptimize.managed", "123")
//      }
//    )
//    val ex1 =
//      intercept[TableMaterializationException] {
//        materializeGraph(graph1)
//      }
//
//    assert(ex1.cause.isInstanceOf[IllegalArgumentException])
//    assert(ex1.cause.getMessage.contains("pipelines.autoOptimize.managed"))
//
//    // Invalid delta property
//    val graph2 = DataflowGraph(
//      new TestGraphRegistrationContext(spark) {
//        registerTable("a")
//          .query(Seq(1).toDF())
//          .tableProperty("delta.enableExpiredLogCleanup", "123")
//      }
//    )
//    val ex2 = intercept[TableMaterializationException] {
//      materializeGraph(graph2)
//    }
//    assert(ex2.cause.isInstanceOf[IllegalArgumentException])
//  }
//
//  test("Spark confs are applied when a table is materialized") {
//
//    val graph =
//      materializeGraph(
//        DataflowGraph(
//          new TestGraphRegistrationContext(spark) {
//            registerTable("a")
//              .query(spark.readStream.format("rate").load())
//              .sparkConf("spark.databricks.delta.properties.defaults.appendOnly", "true")
//            registerTable("b")
//              .query(readStream("a"))
//              .sparkConf("spark.databricks.delta.properties.defaults.appendOnly", "false")
//          }
//        )
//      )
//
//    def validateAppendOnly(tableName: String, expected: Boolean): Unit = {
//      val config = DeltaLog
//        .forTable(spark, graph.tableByName(materializationName(tableName)).path)
//        .getChanges(0)
//        .flatMap(_._2)
//        .collect { case m: Metadata => m }
//        .next
//        .configuration
//      assert(config.get("delta.appendOnly").contains(expected.toString))
//    }
//
//    validateAppendOnly(tableName = "a", expected = true)
//    validateAppendOnly(tableName = "b", expected = false)
//  }
//
//  /**
//   * Creates a Delta table with the provided table properties.
//   *
//   * @param path       The path to the Delta table.
//   * @param properties The properties that are set on the Delta table.
//   */
//  private def createDeltaTableWithProperties(
//                                              path: String,
//                                              properties: Map[String, String]): Unit = {
//    val deltaLog = DeltaLog.forTable(spark, path)
//    val txn = deltaLog.startTransaction()
//    txn.readWholeTable()
//    val metadata = txn.metadata.copy(configuration = properties)
//    txn.updateMetadata(metadata)
//    txn.commit(Nil, DeltaOperations.ManualUpdate)
//  }
//
//  /**
//   * Creates a Delta table that is owned by a pipeline.
//   *
//   * @param path       The path to the Delta table.
//   * @param pipelineId The id of the pipeline that manages this table.
//   */
//  private def createDeltaTableOwnedByPipeline(path: String, pipelineId: String): Unit = {
//    createDeltaTableWithProperties(path, Map("pipelines.pipelineId" -> pipelineId))
//  }
//
//  class SimpleRateStreamPipeline extends Pipeline {
//    registerTable("a").query(spark.readStream.format("rate").load())
//  }
//
//  test(
//    "Materialization succeeds even if there are unknown pipeline properties on the existing table"
//  ) {
//    val rawGraph = DataflowGraph(new SimpleRateStreamPipeline)
//    val (graph1, _, _) = executeInitializingStage(rawGraph)
//    // Add a property to the delta table.
//    val tablePath = graph1.tableByName(materializationName("a")).path
//    createDeltaTableWithProperties(
//      tablePath,
//      Map("pipelines.someProperty" -> "foo")
//    )
//    val deltaLog = DeltaLog.forTable(spark, tablePath)
//    val tableProps = deltaLog.snapshot.metadata.configuration
//    assert(tableProps("pipelines.someProperty") == "foo")
//
//    // Check that table property still exists after second update.
//    materializeGraph(rawGraph)
//    // Existing properties are overwritten
//    val deltaLog2 = DeltaLog.forTable(spark, tablePath)
//    val tableProps2 = deltaLog2.snapshot.metadata.configuration
//    assert(tableProps2.get("pipelines.someProperty").isEmpty)
//  }
//
//  test("Existing tables with no pipelineId property are updated with the correct pipelineId") {
//    val rawGraph = DataflowGraph(new SimpleRateStreamPipeline)
//    val (graph, _, _) = executeInitializingStage(rawGraph)
//    val tablePath = graph.tableByName(materializationName("a")).path
//    // Simulate an existing delta table with no pipelineId
//    spark.range(1, 3).write.format("delta").mode("overwrite").save(tablePath)
//    materializeGraph(rawGraph)
//
//    assertNoDeprecationEvents()
//    val deltaLog = DeltaLog.forTable(spark, tablePath)
//    val tableProps = deltaLog.snapshot.metadata.configuration
//    assert(tableProps("pipelines.pipelineId") == updateContext.defaultOrigin.getPipelineId)
//  }
//
//  for (isFullRefresh <- Seq(true, false)) {
//    test(
//      s"Complete tables should not evolve schema - isFullRefresh = $isFullRefresh"
//    ) {
//      val rawGraph = DataflowGraph(
//        new TestGraphRegistrationContext(spark) {
//          registerView("a")
//            .query(Seq((1, 2), (2, 3)).toDF("x", "y"))
//          registerTable("b")
//            .query(read("a").select("x"))
//        }
//      )
//      val (graph, _, _) = executeInitializingStage(rawGraph)
//      val deltaLog = DeltaLog.forTable(spark, graph.tableByName(materializationName("b")).path)
//      val (refreshSelection, fullRefreshSelection) = if (isFullRefresh) {
//        (NoTables, AllTables)
//      } else {
//        (AllTables, NoTables)
//      }
//      // Make sure to keep the same storage root when updating the UpdateContext
//      updatePipelineUpdateContext(
//        _.copy(
//          refreshTables = refreshSelection,
//          fullRefreshTables = fullRefreshSelection,
//          inputStorageRoot = Option(updateContext.storageRoot)
//        )
//      )
//      materializeGraph(rawGraph)
//
//      assert(deltaLog.startTransaction().metadata.schema == new StructType().add("x", IntegerType))
//
//      materializeGraph(
//        DataflowGraph(
//          new TestGraphRegistrationContext(spark) {
//            registerView("a")
//              .query(Seq((1, 2), (2, 3)).toDF("x", "y"))
//            registerTable("b")
//              .query(read("a").select("y"))
//          }
//        )
//      )
//      assert(deltaLog.startTransaction().metadata.schema == new StructType().add("y", IntegerType))
//    }
//  }
//
//  for (isFullRefresh <- Seq(true, false)) {
//    test(
//      s"Incremental tables should evolve schema only if not full refresh = $isFullRefresh"
//    ) {
//      val streamInts = MemoryStream[Int]
//      streamInts.addData(1 until 5: _*)
//
//      val rawGraph = DataflowGraph(
//        new TestGraphRegistrationContext(spark) {
//          registerView("a")
//            .query(streamInts.toDF())
//          registerTable("b")
//            .query(readStream("a").toDF("x"))
//        }
//      )
//
//      val (graph, _, _) = executeInitializingStage(rawGraph)
//      val deltaLog = DeltaLog.forTable(spark, graph.tableByName(materializationName("b")).path)
//      val (refreshSelection, fullRefreshSelection) = if (isFullRefresh) {
//        (NoTables, AllTables)
//      } else {
//        (AllTables, NoTables)
//      }
//      // Make sure to keep the same storage root when updating the UpdateContext
//      updatePipelineUpdateContext(
//        _.copy(
//          refreshTables = refreshSelection,
//          fullRefreshTables = fullRefreshSelection,
//          inputStorageRoot = Option(updateContext.storageRoot)
//        )
//      )
//      materializeGraph(rawGraph)
//      assert(deltaLog.startTransaction().metadata.schema == new StructType().add("x", IntegerType))
//
//      materializeGraph(
//        DataflowGraph(
//          new TestGraphRegistrationContext(spark) {
//            registerView("a")
//              .query(streamInts.toDF())
//            registerTable("b")
//              .query(readStream("a").toDF("y"))
//          }
//        )
//      )
//
//      if (isFullRefresh) {
//        assert(
//          deltaLog.startTransaction().metadata.schema == new StructType().add("y", IntegerType)
//        )
//      } else {
//        assert(
//          deltaLog.startTransaction().metadata.schema == new StructType()
//            .add("x", IntegerType)
//            .add("y", IntegerType)
//        )
//      }
//    }
//  }
//
//  test(
//    "materialize only selected tables"
//  ) {
//    updatePipelineUpdateContext(
//      _.copy(
//        refreshTables = SomeTables(Set(TableIdentifier("a"))),
//        fullRefreshTables = SomeTables(Set(TableIdentifier("c")))
//      )
//    )
//    val graph =
//      materializeGraph(
//        DataflowGraph(
//          new TestGraphRegistrationContext(spark) {
//            registerTable("a")
//              .query(Seq((1, 2), (2, 3)).toDF("x", "y"))
//            registerTable("b")
//              .query(read("a").select("x"))
//            registerTable("c")
//              .query(read("a").select("y"))
//          }
//        )
//      )
//    val deltaLogA = DeltaLog.forTable(spark, graph.tableByName(materializationName("a")).path)
//    val deltaLogB = DeltaLog.forTable(spark, graph.tableByName(materializationName("b")).path)
//    val deltaLogC = DeltaLog.forTable(spark, graph.tableByName(materializationName("c")).path)
//
//    val snapshotA = deltaLogA.snapshot
//    assert(snapshotA.version == 0)
//    assert(
//      deltaLogA.startTransaction().metadata.schema == new StructType()
//        .add("x", IntegerType)
//        .add("y", IntegerType)
//    )
//
//    val snapshotB = deltaLogB.snapshot
//    assert(snapshotB.version < 0)
//    assert(deltaLogB.startTransaction().metadata.schema == new StructType())
//
//    val snapshotC = deltaLogC.snapshot
//    assert(snapshotC.version == 0)
//    assert(deltaLogC.startTransaction().metadata.schema == new StructType().add("y", IntegerType))
//  }
//
//  test("tables with arrays and maps") {
//    val rawGraph = DataflowGraph(
//      new TestGraphRegistrationContext(spark) {
//        registerTable("a")
//          .query(sql("select map(1, struct('a', 'b')) m"))
//        registerTable("b")
//          .query(Seq(Array(1, 3, 5), Array(2, 4, 6)).toDF("arr"))
//        registerTable("c")
//          .query(read("a").join(read("b")).where("map_entries(m)[0].key = arr[0]"))
//      }
//    )
//    val graph = materializeGraph(rawGraph)
//    val deltaLogA = DeltaLog.forTable(spark, graph.tableByName(materializationName("a")).path)
//    val deltaLogB = DeltaLog.forTable(spark, graph.tableByName(materializationName("b")).path)
//    val deltaLogC = DeltaLog.forTable(spark, graph.tableByName(materializationName("c")).path)
//
//    // Materialize twice because some logic compares the incoming schema with the previous one.
//    materializeGraph(rawGraph)
//
//    assert(
//      deltaLogA.snapshot.schema ==
//        StructType.fromDDL("m MAP<int, struct<col1: string, col2: string>>")
//    )
//    assert(deltaLogB.snapshot.schema == StructType.fromDDL("arr ARRAY<int>"))
//    assert(
//      deltaLogC.snapshot.schema ==
//        StructType.fromDDL("m MAP<int, struct<col1: string, col2: string>>, arr ARRAY<int>")
//    )
//  }
//
//  test("tables with nested arrays and maps") {
//    val rawGraph = DataflowGraph(
//      new TestGraphRegistrationContext(spark) {
//        registerTable("a")
//          .query(sql("select map(0, map(0, struct('a', 'b'))) m"))
//        registerTable("b")
//          .query(sql("select array(array('a', 'b', 'c'), array('d', 'e', 'f')) arr"))
//        registerTable("c")
//          .query(read("a").join(read("b")).where("m[0][0].col1 = arr[0][0]"))
//      }
//    )
//    val graph = materializeGraph(rawGraph)
//    val deltaLogA = DeltaLog.forTable(spark, graph.tableByName(materializationName("a")).path)
//    val deltaLogB = DeltaLog.forTable(spark, graph.tableByName(materializationName("b")).path)
//    val deltaLogC = DeltaLog.forTable(spark, graph.tableByName(materializationName("c")).path)
//
//    // Materialize twice because some logic compares the incoming schema with the previous one.
//    materializeGraph(rawGraph)
//
//    assert(
//      deltaLogA.snapshot.schema ==
//        StructType.fromDDL("m MAP<int, MAP<int, struct<col1: string, col2: string>>>")
//    )
//    assert(deltaLogB.snapshot.schema == StructType.fromDDL("arr ARRAY<ARRAY<string>>"))
//    assert(
//      deltaLogC.snapshot.schema ==
//        StructType.fromDDL(
//          "m MAP<int, MAP<int, struct<col1: string, col2: string>>>, arr ARRAY<ARRAY<string>>"
//        )
//    )
//  }
//
//  test("materializing no tables doesn't throw") {
//    val graph1 =
//      DataflowGraph(flows = Seq.empty, tables = Seq.empty, sinks = Seq.empty, views = Seq.empty)
//    val graph2 = DataflowGraph(
//      flows = Seq(
//       registerFlow(
//          identifier = TableIdentifier("a"),
//          destinationIdentifier = TableIdentifier("a"),
//          currentCatalog = catalogInPipelineSpec,
//          query = dfFlowFunc(Seq((1, 1), (2, 3)).toDF("x", "x2"))
//        )
//      ),
//      tables = Seq(Table(identifier = TableIdentifier("a"), pipeline = dummyPipeline)),
//      views = Seq.empty
//    )
//
//    materializeGraph(graph1)
//    updatePipelineUpdateContext(_.copy(refreshTables = NoTables, fullRefreshTables = NoTables))
//    materializeGraph(graph2)
//  }
//
//  test("Complex data type columns cannot be used as partition columns") {
//    val ex = intercept[TableMaterializationException] {
//      materializeGraph(
//        DataflowGraph(new TestGraphRegistrationContext(spark) {
//          registerTable("tgt")
//            .schema("a LONG, `b.c` STRING, b STRUCT<c: INT>")
//            .partitionBy("b")
//            .query(spark.range(5).selectExpr("1L a", "'text' `b.c`", "named_struct('c', 1) b"))
//        })
//      )
//    }
//    val cause = ex.cause.asInstanceOf[AnalysisException]
//    // The error messages in different DBRs are slightly different
//    assert(
//      cause.getErrorClass == "DELTA_INVALID_PARTITION_COLUMN_TYPE" ||
//        cause.getErrorClass == "INVALID_PARTITION_COLUMN_DATA_TYPE"
//    )
//    val exStr = exceptionString(cause)
//    assert(
//      exStr.contains("Cannot use \"STRUCT<c: INT>\" for partition column") ||
//        exStr.contains("partition column is not supported")
//    )
//  }
//  scalastyle:on
}
