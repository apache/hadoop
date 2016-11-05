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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;

/**
 * Node Manager's register response.
 */
public abstract class RegisterNodeManagerResponse {
  public abstract MasterKey getContainerTokenMasterKey();

  public abstract void setContainerTokenMasterKey(MasterKey secretKey);

  public abstract MasterKey getNMTokenMasterKey();

  public abstract void setNMTokenMasterKey(MasterKey secretKey);

  public abstract NodeAction getNodeAction();

  public abstract void setNodeAction(NodeAction nodeAction);

  public abstract long getRMIdentifier();

  public abstract void setRMIdentifier(long rmIdentifier);

  public abstract String getDiagnosticsMessage();

  public abstract void setDiagnosticsMessage(String diagnosticsMessage);

  public abstract void setRMVersion(String version);

  public abstract String getRMVersion();

  public abstract Resource getResource();

  public abstract void setResource(Resource resource);

  public abstract boolean getAreNodeLabelsAcceptedByRM();

  public abstract void setAreNodeLabelsAcceptedByRM(
      boolean areNodeLabelsAcceptedByRM);
}
