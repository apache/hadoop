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

package org.apache.hadoop.fs.audit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants related to auditing.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class AuditConstants {

  private AuditConstants() {
  }

  /**
   * The host from where requests originate: {@value}.
   * example.org is used as the IETF require that it never resolves.
   * This isn't always met by some mobile/consumer DNS services, but
   * we don't worry about that. What is important is that
   * a scan for "example.org" in the logs will exclusively find
   * entries from this referrer.
   */
  public static final String REFERRER_ORIGIN_HOST = "audit.example.org";

  /**
   * Header: Command: {@value}.
   * Set by tool runner.
   */
  public static final String PARAM_COMMAND = "cm";

  /**
   * Header: FileSystem ID: {@value}.
   */
  public static final String PARAM_FILESYSTEM_ID = "fs";

  /**
   * Header: operation ID: {@value}.
   */
  public static final String PARAM_ID = "id";

  /**
   * JobID query header: {@value}.
   */
  public static final String PARAM_JOB_ID = "ji";

  /**
   * Header: operation: {@value}.
   * These should be from StoreStatisticNames or similar,
   * and are expected to be at the granularity of FS
   * API operations.
   */
  public static final String PARAM_OP = "op";

  /**
   * Header: first path of operation: {@value}.
   */
  public static final String PARAM_PATH = "p1";

  /**
   * Header: second path of operation: {@value}.
   */
  public static final String PARAM_PATH2 = "p2";

  /**
   * Header: Principal: {@value}.
   */
  public static final String PARAM_PRINCIPAL = "pr";

  /**
   * Header: Process ID: {@value}.
   */
  public static final String PARAM_PROCESS = "ps";

  /**
   * Header: Range for GET request data: {@value}.
   */
  public static final String PARAM_RANGE = "rg";

  /**
   * Task Attempt ID query header: {@value}.
   */
  public static final String PARAM_TASK_ATTEMPT_ID = "ta";

  /**
   * Thread 0: the thread which created a span {@value}.
   */
  public static final String PARAM_THREAD0 = "t0";

  /**
   * Thread 1: the thread making the S3 request: {@value}.
   */
  public static final String PARAM_THREAD1 = "t1";

  /**
   * Timestamp of span creation: {@value}.
   */
  public static final String PARAM_TIMESTAMP = "ts";

  /**
   * Num of files to be deleted as part of the bulk delete request.
   */
  public static final String DELETE_KEYS_SIZE = "ks";

}
