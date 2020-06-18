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
package org.apache.hadoop.tools.fedbalance.procedure;

import org.apache.hadoop.conf.Configurable;

import java.io.IOException;

/**
 * The Journal of the state machine. It handles the job persistence and recover.
 */
public interface BalanceJournal extends Configurable {

  /**
   * Save journal of this job.
   */
  void saveJob(BalanceJob job) throws IOException;

  /**
   * Recover the job from journal.
   */
  void recoverJob(BalanceJob job) throws IOException;

  /**
   * List all unfinished jobs.
   */
  BalanceJob[] listAllJobs() throws IOException;

  /**
   * Clear all the journals of this job.
   */
  void clear(BalanceJob job) throws IOException;
}
