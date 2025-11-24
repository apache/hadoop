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

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.transfer.s3.progress.TransferListener;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.store.audit.ActiveThreadSpanSource;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;
import org.apache.hadoop.service.Service;


/**
 * Interface for Audit Managers auditing operations through the
 * AWS libraries.
 * The Audit Manager is the binding between S3AFS and the instantiated
 * plugin point -it adds:
 * <ol>
 *   <li>per-thread tracking of audit spans </li>
 *   <li>The wiring up to the AWS SDK</li>
 *   <li>State change tracking for copy operations (does not address issue)</li>
 * </ol>
 */
@InterfaceAudience.Private
public interface AuditManagerS3A extends Service,
    AuditSpanSource<AuditSpanS3A>,
    AWSAuditEventCallbacks,
    ActiveThreadSpanSource<AuditSpanS3A> {

  /**
   * Get the auditor; valid once initialized.
   * @return the auditor.
   */
  OperationAuditor getAuditor();

  /**
   * Create the execution interceptor(s) for this audit service.
   * The list returned is mutable; new interceptors may be added.
   * @return list of interceptors for the SDK.
   * @throws IOException failure.
   */
  List<ExecutionInterceptor> createExecutionInterceptors() throws IOException;

  /**
   * Return a transfer callback which
   * fixes the active span context to be that in which
   * the transfer listener was created.
   * This can be used to audit the creation of the multipart
   * upload initiation request which the transfer manager
   * makes when a file to be copied is split up.
   * This must be invoked/used within the active span.
   * @return a transfer listener.
   */
  TransferListener createTransferListener();

  /**
   * Check for permission to access a path.
   * The path is fully qualified and the status is the
   * status of the path.
   * This is called from the {@code FileSystem.access()} command
   * and is a soft permission check used by Hive.
   * @param path path to check
   * @param status status of the path.
   * @param mode access mode.
   * @return true if access is allowed.
   * @throws IOException failure
   */
  boolean checkAccess(Path path, S3AFileStatus status, FsAction mode)
      throws IOException;

  /**
   * Update audit flags, especially the out of span rejection option.
   * @param flags audit flags.
   */
  void setAuditFlags(EnumSet<AuditorFlags> flags);
}
