/*
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

package org.apache.hadoop.fs.s3a.audit;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * A source of audit spans.
 */
public interface AuditSpanSource {

  /**
   * Create a span for an operation.
   *
   * All operation names <i>SHOULD</i> come from
   * {@code StoreStatisticNames} or
   * {@code StreamStatisticNames}.
   * @param name operation name.
   * @param path1 first path of operation
   * @param path2 second path of operation
   * @return a span for the audit
   * @throws IOException failure
   */
  AuditSpan createSpan(String name,
      @Nullable String path1,
      @Nullable String path2)
      throws IOException;
}
