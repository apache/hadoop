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

package org.apache.hadoop.yarn.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

public class ContainerTokenIdentifier extends TokenIdentifier {

  private static Log LOG = LogFactory
      .getLog(ContainerTokenIdentifier.class);

  public static final Text KIND = new Text("ContainerToken");

  private ContainerId containerId;
  private String nmHostName;
  private Resource resource;

  public ContainerTokenIdentifier(ContainerId containerID, String hostName, Resource r) {
    this.containerId = containerID;
    this.nmHostName = hostName;
    this.resource = r;
  }

  public ContainerTokenIdentifier() {
  }

  public ContainerId getContainerID() {
    return containerId;
  }

  public String getNmHostName() {
    return nmHostName;
  }

  public Resource getResource() {
    return resource;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    LOG.debug("Writing ContainerTokenIdentifier to RPC layer");
    out.writeInt(this.containerId.getAppId().getId());
    out.writeInt(this.containerId.getAppAttemptId().getAttemptId());
    out.writeInt(this.containerId.getId());
    // TODO: Cluster time-stamp?
    out.writeUTF(this.nmHostName);
    out.writeInt(this.resource.getMemory()); // TODO: more resources.
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.containerId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ContainerId.class);
    this.containerId.setAppId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationId.class));
    this.containerId.setAppAttemptId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationAttemptId.class));
    this.containerId.getAppId().setId(in.readInt());
    this.containerId.getAppAttemptId().setApplicationId(this.containerId.getAppId());
    this.containerId.getAppAttemptId().setAttemptId(in.readInt());
    this.containerId.setId(in.readInt());
    this.nmHostName = in.readUTF();
    this.resource = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Resource.class);
    this.resource.setMemory(in.readInt()); // TODO: more resources.
  }

  @Override
  public Text getKind() {
    return this.KIND;
  }

  @Override
  public UserGroupInformation getUser() {
    return UserGroupInformation.createRemoteUser(this.containerId.toString());
  }

}
