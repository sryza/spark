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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.utils.PipelineTest

class GraphFilterSuite extends PipelineTest {

  private case class NonEmptyTestData(tableFilter: TableFilter, expectedNonEmpty: Boolean)

  namedGridTest("TableFilter.nonEmpty should behave correctly")(
    Map(
      "AllTables should always be non-empty" ->
      NonEmptyTestData(AllTables, expectedNonEmpty = true),
      "NoTables should always be empty" ->
      NonEmptyTestData(NoTables, expectedNonEmpty = false),
      "SomeTables should be empty if no tables provided" ->
      NonEmptyTestData(SomeTables(Set.empty), expectedNonEmpty = false),
      "SomeTables should be non-empty if one table provided" ->
      NonEmptyTestData(SomeTables(Set(TableIdentifier("table1"))), expectedNonEmpty = true),
      "SomeTables should be non-empty if multiple tables provided" ->
      NonEmptyTestData(
        SomeTables(Set(TableIdentifier("table1"), TableIdentifier("table 2"))),
        expectedNonEmpty = true
      )
    )
  ) { testData =>
    assert(testData.tableFilter.nonEmpty == testData.expectedNonEmpty)
  }
}
