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

public interface RegisterNodeManagerResponse {
  MasterKey getContainerTokenMasterKey();

  void setContainerTokenMasterKey(MasterKey secretKey);

  MasterKey getNMTokenMasterKey();

  void setNMTokenMasterKey(MasterKey secretKey);

  NodeAction getNodeAction();

  void setNodeAction(NodeAction nodeAction);

  long getRMIdentifier();

  void setRMIdentifier(long rmIdentifier);

  String getDiagnosticsMessage();

  void setDiagnosticsMessage(String diagnosticsMessage);

  void setRMVersion(String version);

  String getRMVersion();

  Resource getResource();
  void setResource(Resource resource);

  boolean getAreNodeLabelsAcceptedByRM();
  void setAreNodeLabelsAcceptedByRM(boolean areNodeLabelsAcceptedByRM);
}
