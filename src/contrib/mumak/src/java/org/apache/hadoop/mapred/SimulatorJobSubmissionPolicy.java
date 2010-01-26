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
package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;

/**
 * Job submission policies. The set of policies is closed and encapsulated in
 * {@link SimulatorJobSubmissionPolicy}. The handling of submission policies is
 * embedded in the {@link SimulatorEngine} (through various events).
 * 
 */
public enum SimulatorJobSubmissionPolicy {
  /**
   * replay the trace by following the job inter-arrival rate faithfully.
   */
  REPLAY,
  
  /**
   * ignore submission time, keep submitting jobs until the cluster is saturated.
   */
  STRESS,
  
  /**
   * submitting jobs sequentially.
   */
  SERIAL;
  
  public static final String JOB_SUBMISSION_POLICY = "mumak.job-submission.policy";

  static public SimulatorJobSubmissionPolicy getPolicy(Configuration conf) {
    String policy = conf.get(JOB_SUBMISSION_POLICY, REPLAY.name());
    return valueOf(policy.toUpperCase());
  }
}
