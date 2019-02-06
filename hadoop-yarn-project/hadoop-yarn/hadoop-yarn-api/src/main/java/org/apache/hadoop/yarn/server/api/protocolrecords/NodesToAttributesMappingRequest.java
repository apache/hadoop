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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * list of node-attribute mapping request info.
 */
@Public
@Unstable
public abstract class NodesToAttributesMappingRequest {

  public static NodesToAttributesMappingRequest newInstance(
      AttributeMappingOperationType operation,
      List<NodeToAttributes> nodesToAttributes, boolean failOnUnknownNodes) {
    NodesToAttributesMappingRequest request =
        Records.newRecord(NodesToAttributesMappingRequest.class);
    request.setNodesToAttributes(nodesToAttributes);
    request.setFailOnUnknownNodes(failOnUnknownNodes);
    request.setOperation(operation);
    return request;
  }

  @Public
  @Unstable
  public abstract void setNodesToAttributes(
      List<NodeToAttributes> nodesToAttributes);

  @Public
  @Unstable
  public abstract List<NodeToAttributes> getNodesToAttributes();

  @Public
  @Unstable
  public abstract void setFailOnUnknownNodes(boolean failOnUnknownNodes);

  @Public
  @Unstable
  public abstract boolean getFailOnUnknownNodes();

  @Public
  @Unstable
  public abstract void setOperation(AttributeMappingOperationType operation);

  @Public
  @Unstable
  public abstract AttributeMappingOperationType getOperation();
}
