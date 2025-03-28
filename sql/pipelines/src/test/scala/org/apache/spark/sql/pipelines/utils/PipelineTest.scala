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

package org.apache.spark.sql.pipelines.utils

import scala.util.{Failure, Try}
import scala.util.control.NonFatal

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Tag}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.SparkSession.{clearActiveSession, setActiveSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.{DataFrame, Dataset, SparkSession, SQLContext}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.pipelines.graph.{DataflowGraph, PipelineUpdateContextImpl, SqlGraphRegistrationContext}
import org.apache.spark.sql.pipelines.utils.PipelineTest.cleanupMetastore

abstract class PipelineTest
    extends SparkFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers
    with SparkErrorTestMixin
    with TargetCatalogAndSchemaMixin
    with Logging {

  var spark: SparkSession = createAndInitializeSpark()
  val originalSpark: SparkSession = spark.cloneSession()

  implicit def sqlContext: SQLContext = spark.sqlContext
  def sql(text: String): DataFrame = spark.sql(text)

  protected def startPipelineAndWaitForCompletion(unresolvedDataflowGraph: DataflowGraph): Unit = {
    val updateContext = new PipelineUpdateContextImpl(
      unresolvedDataflowGraph, eventCallback = _ => ())
    updateContext.pipelineExecution.runPipeline()
    updateContext.pipelineExecution.awaitCompletion()
  }

  protected case class TestSqlFile(sqlText: String, sqlFilePath: String)
  protected def unresolvedDataflowGraphFromSqlFiles(
      sqlFiles: Seq[TestSqlFile]
  ): DataflowGraph = {
    val graphRegistrationContext = new TestGraphRegistrationContext(spark)
    sqlFiles.foreach { sqlFile =>
      new SqlGraphRegistrationContext(graphRegistrationContext).processSqlFile(
        sqlText = sqlFile.sqlText,
        sqlFilePath = sqlFile.sqlFilePath,
        spark = spark
      )
    }
    graphRegistrationContext
      .toDataflowGraph
  }

  protected def unresolvedDataflowGraphFromSql(
      sqlText: String
  ): DataflowGraph = {
    val graphRegistrationContext = new TestGraphRegistrationContext(spark)
    new SqlGraphRegistrationContext(graphRegistrationContext).processSqlFile(
      sqlText = sqlText,
      sqlFilePath = "dataset.sql",
      spark = spark
    )
    graphRegistrationContext.toDataflowGraph
  }

  /**
   * Spark confs for [[originalSpark]]. Spark confs set here will be the default spark confs for
   * all spark sessions created in tests.
   */
  protected def sparkConf: SparkConf = {
    var conf = new SparkConf()
      .set("spark.sql.shuffle.partitions", "2")
      .set("spark.sql.session.timeZone", "UTC")

    if (schemaInPipelineSpec.isDefined) {
      conf = conf.set("pipelines.schema", schemaInPipelineSpec.get)
    }

    if (Option(System.getenv("ENABLE_SPARK_UI")).exists(s => java.lang.Boolean.valueOf(s))) {
      conf = conf.set("spark.ui.enabled", "true")
    }
    conf
  }

  /** Returns the dataset name in the event log. */
  protected def eventLogName(
      name: String,
      catalog: Option[String] = catalogInPipelineSpec,
      schema: Option[String] = schemaInPipelineSpec,
      isView: Boolean = false
  ): String = {
    fullyQualifiedIdentifier(name, catalog, schema, isView).unquotedString
  }

  /** Returns the fully qualified identifier. */
  protected def fullyQualifiedIdentifier(
      name: String,
      catalog: Option[String] = catalogInPipelineSpec,
      schema: Option[String] = schemaInPipelineSpec,
      isView: Boolean = false
  ): TableIdentifier = {
    if (isView) {
      TableIdentifier(name)
    } else {
      TableIdentifier(
        catalog = catalog,
        database = schema,
        table = name
      )
    }
  }

  /**
   * This exists temporarily for compatibility with tests that become invalid when multiple
   * executors are available.
   */
  protected def master = "local[*]"

  /** Creates and returns a initialized spark session. */
  def createAndInitializeSpark(): SparkSession = {
    val newSparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .master(master)
      .getOrCreate()
    newSparkSession
  }

  /** Set up the spark session before each test. */
  protected def initializeSparkBeforeEachTest(): Unit = {
    clearActiveSession()
    spark = originalSpark.newSession()
    setActiveSession(spark)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    initializeSparkBeforeEachTest()
    cleanupMetastore(spark)
    (catalogInPipelineSpec, schemaInPipelineSpec) match {
      case (Some(catalog), Some(schema)) =>
        sql(s"CREATE SCHEMA IF NOT EXISTS `$catalog`.`$schema`")
      case _ =>
        schemaInPipelineSpec.foreach(s => sql(s"CREATE SCHEMA IF NOT EXISTS `$s`"))
    }
  }

  override def afterEach(): Unit = {
    cleanupMetastore(spark)
    super.afterEach()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  protected def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
      testFun: A => Unit): Unit = {
    namedGridTest(testNamePrefix, testTags: _*)(params.map(a => a.toString -> a).toMap)(testFun)
  }

  protected def namedGridTest[A](testNamePrefix: String, testTags: Tag*)(params: Map[String, A])(
      testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" (${param._1})", testTags: _*)(testFun(param._2))
    }
  }

  private def checkAnswerAndPlan(
      df: => DataFrame,
      expectedAnswer: Seq[Row],
      checkPlan: Option[SparkPlan => Unit]): Unit = {
    QueryTest.checkAnswer(df, expectedAnswer)

    // To help with test development, you can dump the plan to the log by passing
    // `--test_env=DUMP_PLAN=true` to `bazel test`.
    if (Option(System.getenv("DUMP_PLAN")).exists(s => java.lang.Boolean.valueOf(s))) {
      log.info(s"Spark plan:\n${df.queryExecution.executedPlan}")
    }
    checkPlan.foreach(_.apply(df.queryExecution.executedPlan))
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    checkAnswerAndPlan(df, expectedAnswer, None)
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(df, Seq(expectedAnswer))
  }

  case class ValidationArgs(
      ignoreFieldOrder: Boolean = false,
      ignoreFieldCase: Boolean = false
  )

  protected def checkDatasetUnorderly[T: Ordering](ds: => Dataset[T], expectedAnswer: T*): Unit = {
    val result = getResult(ds)
    if (!QueryTest.compare(result.toSeq.sorted, expectedAnswer.sorted)) {
      fail(s"""
              |Decoded objects do not match expected objects:
              |expected: $expectedAnswer
              |actual:   ${result.toSeq}
         """.stripMargin)
    }
  }

  private def getResult[T](ds: => Dataset[T]): Array[T] = {
    ds

    try ds.collect()
    catch {
      case NonFatal(e) =>
        fail(
          s"""
             |Exception collecting dataset as objects
             |${ds.queryExecution}
           """.stripMargin,
          e
        )
    }
  }

  /**
   * Helper method to verify unresolved column error message. We expect three elements to be present
   * in the message: error class, unresolved column name, list of suggested columns.
   */
  protected def verifyUnresolveColumnError(
      errorMessage: String,
      unresolved: String,
      suggested: Seq[String]): Unit = {
    assert(errorMessage.contains(unresolved))
    assert(
      errorMessage.contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]") ||
      errorMessage.contains("[MISSING_COLUMN]")
    )
    suggested.foreach { x =>
      if (errorMessage.contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")) {
        assert(errorMessage.contains(s"`$x`"))
      } else {
        assert(errorMessage.contains(x))
      }
    }
  }
}

/**
 * A trait that provides a way to specify the target catalog and schema for a test.
 */
trait TargetCatalogAndSchemaMixin {

  protected def catalogInPipelineSpec: Option[String] = Option(
    TestGraphRegistrationContext.DEFAULT_CATALOG
  )

  protected def schemaInPipelineSpec: Option[String] = Option(
    TestGraphRegistrationContext.DEFAULT_DATABASE
  )
}

object PipelineTest extends Logging {
  /** System schemas per-catalog that's can't be directly deleted. */
  protected val systemSchemas: Set[String] = Set("default", "information_schema")

  /** System catalogs that are read-only and cannot be modified/dropped. */
  private val systemCatalogs: Set[String] = Set("samples")

  /** Catalogs that cannot be dropped but schemas or tables under it can be cleaned up. */
  private val undroppableCatalogs: Set[String] = Set(
    "hive_metastore",
    "spark_catalog",
    "system",
    "main"
  )

  /**
   * Try to drop the schema in the catalog and return whether it is successfully dropped.
   */
  private def dropSchemaIfPossible(
      spark: SparkSession,
      catalogName: String,
      schemaName: String): Boolean = {
    try {
      spark.sql(s"DROP SCHEMA IF EXISTS `$catalogName`.`$schemaName` CASCADE")
      true
    } catch {
      case NonFatal(e) =>
        logInfo(
          s"Failed to drop schema $schemaName in catalog $catalogName, ex:${e.getMessage}"
        )
        false
    }
  }

  /** Cleanup resources created in the metastore by tests. */
  def cleanupMetastore(spark: SparkSession): Unit = synchronized {
    // some tests stop the spark session and managed the cleanup by themself, so no need to
    // cleanup if no active spark session found
    if (spark.sparkContext.isStopped) {
      return
    }
    val catalogs =
      spark.sql(s"SHOW CATALOGS").collect().map(_.getString(0)).filterNot(systemCatalogs.contains)
    catalogs.foreach { catalog =>
      if (undroppableCatalogs.contains(catalog)) {
        val schemas =
          spark.sql(s"SHOW SCHEMAS IN `$catalog`").collect().map(_.getString(0))
        schemas.foreach { schema =>
          if (systemSchemas.contains(schema) || !dropSchemaIfPossible(spark, catalog, schema)) {
            spark
              .sql(s"SHOW tables in `$catalog`.`$schema`")
              .collect()
              .map(_.getString(0))
              .foreach { table =>
                Try(spark.sql(s"DROP table IF EXISTS `$catalog`.`$schema`.`$table`")) match {
                  case Failure(e) =>
                    logInfo(
                      s"Failed to drop table $table in schema $schema in catalog $catalog, " +
                      s"ex:${e.getMessage}"
                    )
                  case _ =>
                }
              }
          }
        }
      } else {
        Try(spark.sql(s"DROP CATALOG IF EXISTS `$catalog` CASCADE")) match {
          case Failure(e) =>
            logInfo(s"Failed to drop catalog $catalog, ex:${e.getMessage}")
          case _ =>
        }
      }
    }
    spark.sessionState.catalog.reset()
  }
}
