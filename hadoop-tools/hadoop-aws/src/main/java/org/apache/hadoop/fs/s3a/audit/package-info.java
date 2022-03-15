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

/**
 * Support for auditing and ultimately tracing operations.
 * This is a public API for extension points, e.g. opentracing.
 * However, it is very unstable as we evolve how best to audit/trace
 * operation.
 *
 * An audit service is instantiated when an S3A Filesystem is initialized
 * during creation.
 * The choice of service is determined in the configuration option
 * {@link org.apache.hadoop.fs.s3a.audit.S3AAuditConstants#AUDIT_SERVICE_CLASSNAME}.
 * The service MUST implement the interface
 * {@link org.apache.hadoop.fs.s3a.audit.OperationAuditor}
 * to provide an {@link org.apache.hadoop.fs.store.audit.AuditSpan} whenever
 * an operation is started through a public FileSystem API call
 * (+some other operations).
 */

@InterfaceAudience.LimitedPrivate("S3A auditing extensions")
@InterfaceStability.Unstable
package org.apache.hadoop.fs.s3a.audit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;