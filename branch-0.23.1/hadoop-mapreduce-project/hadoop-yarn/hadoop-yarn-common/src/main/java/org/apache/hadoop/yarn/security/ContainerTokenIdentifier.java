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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.BuilderUtils;

public class ContainerTokenIdentifier extends TokenIdentifier {

  private static Log LOG = LogFactory.getLog(ContainerTokenIdentifier.class);

  public static final Text KIND = new Text("ContainerToken");

  private ContainerId containerId;
  private String nmHostAddr;
  private Resource resource;

  public ContainerTokenIdentifier(ContainerId containerID, String hostName,
      Resource r) {
    this.containerId = containerID;
    this.nmHostAddr = hostName;
    this.resource = r;
  }

  public ContainerTokenIdentifier() {
  }

  public ContainerId getContainerID() {
    return this.containerId;
  }

  public String getNmHostAddress() {
    return this.nmHostAddr;
  }

  public Resource getResource() {
    return this.resource;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    LOG.debug("Writing ContainerTokenIdentifier to RPC layer: " + this);
    ApplicationAttemptId applicationAttemptId = this.containerId
        .getApplicationAttemptId();
    ApplicationId applicationId = applicationAttemptId.getApplicationId();
    out.writeLong(applicationId.getClusterTimestamp());
    out.writeInt(applicationId.getId());
    out.writeInt(applicationAttemptId.getAttemptId());
    out.writeInt(this.containerId.getId());
    out.writeUTF(this.nmHostAddr);
    out.writeInt(this.resource.getMemory());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    ApplicationId applicationId = BuilderUtils.newApplicationId(
        in.readLong(), in.readInt());
    ApplicationAttemptId applicationAttemptId = BuilderUtils
        .newApplicationAttemptId(applicationId, in.readInt());
    this.containerId = BuilderUtils.newContainerId(applicationAttemptId, in
        .readInt());
    this.nmHostAddr = in.readUTF();
    this.resource = BuilderUtils.newResource(in.readInt());
  }

  @Override
  public Text getKind() {
    return KIND;
  }

  @Override
  public UserGroupInformation getUser() {
    return UserGroupInformation.createRemoteUser(this.containerId.toString());
  }

  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND;
    }
  }
}
