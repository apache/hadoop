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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;

/**
 * It is the interface of writing the application history, exposing the methods
 * of writing {@link ApplicationStartData}, {@link ApplicationFinishData}
 * {@link ApplicationAttemptStartData}, {@link ApplicationAttemptFinishData},
 * {@link ContainerStartData} and {@link ContainerFinishData}.
 */
@Private
@Unstable
public interface ApplicationHistoryWriter {

  /**
   * This method writes the information of <code>RMApp</code> that is available
   * when it starts.
   * 
   * @param appStart
   *          the record of the information of <code>RMApp</code> that is
   *          available when it starts
   * @throws IOException
   */
  void applicationStarted(ApplicationStartData appStart) throws IOException;

  /**
   * This method writes the information of <code>RMApp</code> that is available
   * when it finishes.
   * 
   * @param appFinish
   *          the record of the information of <code>RMApp</code> that is
   *          available when it finishes
   * @throws IOException
   */
  void applicationFinished(ApplicationFinishData appFinish) throws IOException;

  /**
   * This method writes the information of <code>RMAppAttempt</code> that is
   * available when it starts.
   * 
   * @param appAttemptStart
   *          the record of the information of <code>RMAppAttempt</code> that is
   *          available when it starts
   * @throws IOException
   */
  void applicationAttemptStarted(ApplicationAttemptStartData appAttemptStart)
      throws IOException;

  /**
   * This method writes the information of <code>RMAppAttempt</code> that is
   * available when it finishes.
   * 
   * @param appAttemptFinish
   *          the record of the information of <code>RMAppAttempt</code> that is
   *          available when it finishes
   * @throws IOException
   */
  void
      applicationAttemptFinished(ApplicationAttemptFinishData appAttemptFinish)
          throws IOException;

  /**
   * This method writes the information of <code>RMContainer</code> that is
   * available when it starts.
   * 
   * @param containerStart
   *          the record of the information of <code>RMContainer</code> that is
   *          available when it starts
   * @throws IOException
   */
  void containerStarted(ContainerStartData containerStart) throws IOException;

  /**
   * This method writes the information of <code>RMContainer</code> that is
   * available when it finishes.
   * 
   * @param containerFinish
   *          the record of the information of <code>RMContainer</code> that is
   *          available when it finishes
   * @throws IOException
   */
  void containerFinished(ContainerFinishData containerFinish)
      throws IOException;

}
