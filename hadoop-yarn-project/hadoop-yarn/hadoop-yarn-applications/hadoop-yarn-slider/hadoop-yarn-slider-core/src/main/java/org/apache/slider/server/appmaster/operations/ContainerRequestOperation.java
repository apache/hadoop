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

package org.apache.slider.server.appmaster.operations;

import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.slider.server.appmaster.state.ContainerPriority;

/**
 * A container request operation
 */
public class ContainerRequestOperation extends AbstractRMOperation {

  private final AMRMClient.ContainerRequest request;

  public ContainerRequestOperation(AMRMClient.ContainerRequest request) {
    Preconditions.checkArgument(request != null, "Null container request");
    this.request = request;
  }

  public AMRMClient.ContainerRequest getRequest() {
    return request;
  }

  public Priority getPriority() {
    return request.getPriority();
  }

  public  boolean getRelaxLocality() {
    return request.getRelaxLocality();
  }

  @Override
  public void execute(RMOperationHandlerActions handler) {
    handler.addContainerRequest(request);
  }

  @Override
  public String toString() {
    return "request container for role "
        + ContainerPriority.toString(getPriority())
        + " request " + request
        + " relaxLocality=" + getRelaxLocality();
  }
}
