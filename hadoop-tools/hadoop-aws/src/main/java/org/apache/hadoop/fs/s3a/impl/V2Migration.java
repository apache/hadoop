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

package org.apache.hadoop.fs.s3a.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.store.LogExactlyOnce;

import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_REQUEST_HANDLERS;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SDK_V2_UPGRADE_LOG_NAME;

/**
 * This class provides utility methods required for migrating S3A to AWS Java SDK V2.
 * For more information on the upgrade, see HADOOP-18073.
 *
 * <p>in HADOOP-18382. Upgrade AWS SDK to V2 - Prerequisites,
 * this class contained a series of `LogExactlyOnce` loggers to warn on
 * the first use of a feature which would change incompatibly; this shipped in Hadoop 3.3.5.
 * <p>
 * With the move to v2 completed, attempts to use the v1 classes, will fail
 * -except for the special case of support for v1 credential providers.
 * <p>
 * The warning methods are still present, where appropriate, but downgraded to debug
 * and only retained for debugging migration issues.
 */
public final class V2Migration {

  private V2Migration() { }

  public static final Logger SDK_V2_UPGRADE_LOG = LoggerFactory.getLogger(SDK_V2_UPGRADE_LOG_NAME);

  private static final LogExactlyOnce WARN_OF_REQUEST_HANDLERS =
      new LogExactlyOnce(SDK_V2_UPGRADE_LOG);

  /**
   * Notes use of request handlers.
   * @param handlers handlers declared
   */
  public static void v1RequestHandlersUsed(final String handlers) {
    WARN_OF_REQUEST_HANDLERS.warn(
        "Ignoring V1 SDK request handlers set in {}: {}",
        AUDIT_REQUEST_HANDLERS, handlers);
  }

}
