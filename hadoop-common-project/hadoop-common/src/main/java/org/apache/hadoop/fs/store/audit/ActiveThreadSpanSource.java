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

package org.apache.hadoop.fs.store.audit;

/**
 * Interface to get the active thread span.
 * This can be used to collect the active span to
 * propagate it into other threads.
 *
 * FileSystems which track their active span may implement
 * this and offer their active span.
 */
public interface ActiveThreadSpanSource<T extends AuditSpan> {

  /**
   * The active span. This may not be a valid span, i.e. there is no guarantee
   * that {@code getActiveAuditSpan().isValidSpan()} is true, but
   * implementations MUST always return a non-null span.
   * @return the currently active span.
   */
  T getActiveAuditSpan();
}
