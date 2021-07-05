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

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * A marker attribute simply to highlight which of the methods
 * in a FileSystem why are audit entry points.
 * <ol>
 *   <li>
 *     A FS method is an AuditEntryPoint if, on invocation it
 *     creates and activates an Audit Span for that FS.
 *   </li>
 *   <li>
 *     The audit span SHOULD be deactivated before returning,
 *   </li>
 *   <li>
 *     Objects returned by the API call which go on
 *     to make calls of the filesystem MUST perform
 *     all IO within the same audit span.
 *   </li>
 *   <li>
 *     Audit Entry points SHOULD NOT invoke other Audit Entry Points.
 *     This is to ensure the original audit span information
 *     is not replaced.
 *   </li>
 * </ol>
 * FileSystem methods the entry point then invokes
 * SHOULD NOT invoke audit entry points internally.
 *
 * All external methods MUST be audit entry points.
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
public @interface AuditEntryPoint {
}
