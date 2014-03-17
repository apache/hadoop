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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;

public class ApplicationHistoryStoreTestUtils {

  protected ApplicationHistoryStore store;

  protected void writeApplicationStartData(ApplicationId appId)
      throws IOException {
    store.applicationStarted(ApplicationStartData.newInstance(appId,
      appId.toString(), "test type", "test queue", "test user", 0, 0));
  }

  protected void writeApplicationFinishData(ApplicationId appId)
      throws IOException {
    store.applicationFinished(ApplicationFinishData.newInstance(appId, 0,
      appId.toString(), FinalApplicationStatus.UNDEFINED,
      YarnApplicationState.FINISHED));
  }

  protected void writeApplicationAttemptStartData(
      ApplicationAttemptId appAttemptId) throws IOException {
    store.applicationAttemptStarted(ApplicationAttemptStartData.newInstance(
      appAttemptId, appAttemptId.toString(), 0,
      ContainerId.newInstance(appAttemptId, 1)));
  }

  protected void writeApplicationAttemptFinishData(
      ApplicationAttemptId appAttemptId) throws IOException {
    store.applicationAttemptFinished(ApplicationAttemptFinishData.newInstance(
      appAttemptId, appAttemptId.toString(), "test tracking url",
      FinalApplicationStatus.UNDEFINED, YarnApplicationAttemptState.FINISHED));
  }

  protected void writeContainerStartData(ContainerId containerId)
      throws IOException {
    store.containerStarted(ContainerStartData.newInstance(containerId,
      Resource.newInstance(0, 0), NodeId.newInstance("localhost", 0),
      Priority.newInstance(containerId.getId()), 0));
  }

  protected void writeContainerFinishData(ContainerId containerId)
      throws IOException {
    store.containerFinished(ContainerFinishData.newInstance(containerId, 0,
      containerId.toString(), 0, ContainerState.COMPLETE));
  }

}
