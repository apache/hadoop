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
package org.apache.hadoop.hdfs.procedure;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * This class contains constants for configuration keys and default values
 * used in hdfs procedure.
 */
@InterfaceAudience.Private
public final class BalanceProcedureConfigKeys {
  /* The worker threads number of the BalanceProcedureScheduler */
  public static final String WORK_THREAD_NUM =
      "hadoop.hdfs.procedure.work.thread.num";
  public static final int WORK_THREAD_NUM_DEFAULT = 10;
  /* The uri of the journal */
  public static final String SCHEDULER_JOURNAL_URI =
      "hadoop.hdfs.procedure.scheduler.journal.uri";
  public static final String JOB_PREFIX = "JOB-";
  public static final String TMP_TAIL = ".tmp";
  public static final String JOURNAL_CLASS =
      "hadoop.hdfs.procedure.journal.class";

  private BalanceProcedureConfigKeys() {}
}
