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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.audit.impl.NoopAuditor;

/**
 * Noop auditor which lets access checks be enabled/disabled.
 */
public class AccessCheckingAuditor  extends NoopAuditor {

  public static final String CLASS =
      "org.apache.hadoop.fs.s3a.audit.AccessCheckingAuditor";

  /** Flag to enable/disable access. */
  private boolean accessAllowed = true;

  public AccessCheckingAuditor() {
  }

  public void setAccessAllowed(final boolean accessAllowed) {
    this.accessAllowed = accessAllowed;
  }

  @Override
  public boolean checkAccess(final Path path,
      final S3AFileStatus status,
      final FsAction mode)
      throws IOException {
    return accessAllowed;
  }
}
