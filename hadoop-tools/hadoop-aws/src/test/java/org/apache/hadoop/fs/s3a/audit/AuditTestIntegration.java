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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.audit.impl.NoopAuditManager;
import org.apache.hadoop.fs.s3a.audit.impl.NoopAuditor;

/**
 * Support for auditing in testing.
 */
public class AuditTestIntegration {

  /**
   * Reusable no-op span instance.
   */
  public static AuditSpan NOOP_SPAN = NoopAuditManager
      .createNewSpan("noop", null, null);


  /**
   * Create, init and start a no-op auditor instance.
   * @param conf configuration.
   * @return a started instance.
   */
  public static OperationAuditor noopAuditor(Configuration conf) {
    return NoopAuditor.newInstance(conf, null);
  }
}
