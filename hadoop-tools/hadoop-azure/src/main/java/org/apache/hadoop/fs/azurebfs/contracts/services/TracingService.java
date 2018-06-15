/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.contracts.services;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceScope;

/**
 * Azure Blob File System tracing service.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface TracingService extends InjectableService {
  /**
   * Creates a {@link TraceScope} object with the provided description.
   * @param description the trace description.
   * @return created traceScope.
   */
  TraceScope traceBegin(String description);

  /**
   * Creates a {@link TraceScope} object with the provided description.
   * @param description the trace description.
   * @param parentSpanId the span id of the parent trace scope.
   * @return create traceScope
   */
  TraceScope traceBegin(String description, SpanId parentSpanId);

  /**
   * Gets current thread latest generated traceScope id.
   * @return current thread latest generated traceScope id.
   */
  SpanId getCurrentTraceScopeSpanId();

  /**
   * Appends the provided exception to the trace scope.
   * @param traceScope the scope which exception needs to be attached to.
   * @param azureBlobFileSystemException the exception to be attached to the scope.
   */
  void traceException(TraceScope traceScope, AzureBlobFileSystemException azureBlobFileSystemException);

  /**
   * Ends the provided traceScope.
   * @param traceScope the scope that needs to be ended.
   */
  void traceEnd(TraceScope traceScope);
}