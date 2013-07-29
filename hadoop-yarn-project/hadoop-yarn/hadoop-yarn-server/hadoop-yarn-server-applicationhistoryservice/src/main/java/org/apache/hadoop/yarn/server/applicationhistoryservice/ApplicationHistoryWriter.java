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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;

/**
 * It is the interface of writing the application history, exposing the methods
 * of writing {@link ApplicationHistoryData},
 * {@link ApplicationAttemptHistoryData} and {@link ContainerHistoryData}.
 */
@Private
@Unstable
public interface ApplicationHistoryWriter {

  /**
   * This method persists an {@link ApplicationHistoryData} object.
   * @param app the {@link ApplicationHistoryData} object
   * @throws Throwable
   */
  void writeApplication(ApplicationHistoryData app) throws Throwable;

  /**
   * This method persists an {@link ApplicationAttemptHistoryData} object.
   * @param appAttempt the {@link ApplicationAttemptHistoryData} object
   * @throws Throwable
   */
  void writeApplicationAttempt(
      ApplicationAttemptHistoryData appAttempt) throws Throwable;

  /**
   * This method persists a {@link ContainerHistoryData} object.
   * @param container the {@link ContainerHistoryData} object
   * @throws Throwable
   */
  void writeContainer(ContainerHistoryData container) throws Throwable;

}
