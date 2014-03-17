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

package org.apache.hadoop.yarn.server.applicationhistoryservice.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.util.Records;

/**
 * The class contains the fields that can be determined when
 * <code>RMContainer</code> finishes, and that need to be stored persistently.
 */
@Public
@Unstable
public abstract class ContainerFinishData {

  @Public
  @Unstable
  public static ContainerFinishData newInstance(ContainerId containerId,
      long finishTime, String diagnosticsInfo, int containerExitCode,
      ContainerState containerState) {
    ContainerFinishData containerFD =
        Records.newRecord(ContainerFinishData.class);
    containerFD.setContainerId(containerId);
    containerFD.setFinishTime(finishTime);
    containerFD.setDiagnosticsInfo(diagnosticsInfo);
    containerFD.setContainerExitStatus(containerExitCode);
    containerFD.setContainerState(containerState);
    return containerFD;
  }

  @Public
  @Unstable
  public abstract ContainerId getContainerId();

  @Public
  @Unstable
  public abstract void setContainerId(ContainerId containerId);

  @Public
  @Unstable
  public abstract long getFinishTime();

  @Public
  @Unstable
  public abstract void setFinishTime(long finishTime);

  @Public
  @Unstable
  public abstract String getDiagnosticsInfo();

  @Public
  @Unstable
  public abstract void setDiagnosticsInfo(String diagnosticsInfo);

  @Public
  @Unstable
  public abstract int getContainerExitStatus();

  @Public
  @Unstable
  public abstract void setContainerExitStatus(int containerExitStatus);

  @Public
  @Unstable
  public abstract ContainerState getContainerState();

  @Public
  @Unstable
  public abstract void setContainerState(ContainerState containerState);

}
