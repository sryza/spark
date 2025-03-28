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

package org.apache.spark.sql.pipelines.errors.formatting

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.pipelines.graph.QueryOrigin

class FormattedAnalysisException private[formatting] (
    originalEx: AnalysisException,
    origin: QueryOrigin,
    errorClass: String,
    message: String,
    cause: Throwable)
    extends AnalysisException(
      originalEx.getMessage,
      originalEx.line,
      originalEx.startPosition,
      Option(cause),
      originalEx.errorClass,
      originalEx.messageParameters,
      originalEx.context
    ) {
//    with FormattedThrowable { fixme
  def getOriginalException: Throwable = originalEx

  def getQueryOrigin: QueryOrigin = origin

  override def getMessage: String = message

  override def getErrorClass: String = errorClass
}
