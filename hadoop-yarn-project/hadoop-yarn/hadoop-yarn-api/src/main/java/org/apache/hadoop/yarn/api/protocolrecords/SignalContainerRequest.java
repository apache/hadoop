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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request sent by the client to the <code>ResourceManager</code>
 * or by the <code>ApplicationMaster</code> to the <code>NodeManager</code>
 * to signal a container.
 * @see SignalContainerCommand </p>
 */
@Public
@Evolving
public abstract class SignalContainerRequest {

  @Public
  @Unstable
  public static SignalContainerRequest newInstance(ContainerId containerId,
      SignalContainerCommand signalContainerCommand) {
    SignalContainerRequest request =
        Records.newRecord(SignalContainerRequest.class);
    request.setContainerId(containerId);
    request.setCommand(signalContainerCommand);
    return request;
  }

  /**
   * Get the <code>ContainerId</code> of the container to signal.
   * @return <code>ContainerId</code> of the container to signal.
   */
  @Public
  @Unstable
  public abstract ContainerId getContainerId();

  /**
   * Set the <code>ContainerId</code> of the container to signal.
   */
  @Public
  @Unstable
  public abstract void setContainerId(ContainerId containerId);

  /**
   * Get the <code>SignalContainerCommand</code> of the signal request.
   * @return <code>SignalContainerCommand</code> of the signal request.
   */
  @Public
  @Unstable
  public abstract SignalContainerCommand getCommand();

  /**
   * Set the <code>SignalContainerCommand</code> of the signal request.
   */
  @Public
  @Unstable
  public abstract void setCommand(SignalContainerCommand command);
}
