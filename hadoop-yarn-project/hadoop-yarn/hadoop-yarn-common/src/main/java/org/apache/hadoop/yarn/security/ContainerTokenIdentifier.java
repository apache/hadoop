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
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * TokenIdentifier for a container. Encodes {@link ContainerId},
 * {@link Resource} needed by the container and the target NMs host-address.
 * 
 */
@Public
@Evolving
public class ContainerTokenIdentifier extends TokenIdentifier {

  private static Log LOG = LogFactory.getLog(ContainerTokenIdentifier.class);

  public static final Text KIND = new Text("ContainerToken");

  private ContainerId containerId;
  private String nmHostAddr;
  private String appSubmitter;
  private Resource resource;
  private long expiryTimeStamp;
  private int masterKeyId;
  private long rmIdentifier;
  private Priority priority;
  private long creationTime;

  public ContainerTokenIdentifier(ContainerId containerID,
      String hostName, String appSubmitter, Resource r, long expiryTimeStamp,
      int masterKeyId, long rmIdentifier, Priority priority, long creationTime) {
    this.containerId = containerID;
    this.nmHostAddr = hostName;
    this.appSubmitter = appSubmitter;
    this.resource = r;
    this.expiryTimeStamp = expiryTimeStamp;
    this.masterKeyId = masterKeyId;
    this.rmIdentifier = rmIdentifier;
    this.priority = priority;
    this.creationTime = creationTime;
  }

  /**
   * Default constructor needed by RPC layer/SecretManager.
   */
  public ContainerTokenIdentifier() {
  }

  public ContainerId getContainerID() {
    return this.containerId;
  }

  public String getApplicationSubmitter() {
    return this.appSubmitter;
  }

  public String getNmHostAddress() {
    return this.nmHostAddr;
  }

  public Resource getResource() {
    return this.resource;
  }

  public long getExpiryTimeStamp() {
    return this.expiryTimeStamp;
  }

  public int getMasterKeyId() {
    return this.masterKeyId;
  }

  public Priority getPriority() {
    return this.priority;
  }

  public long getCreationTime() {
    return this.creationTime;
  }
  /**
   * Get the RMIdentifier of RM in which containers are allocated
   * @return RMIdentifier
   */
  public long getRMIdentifer() {
    return this.rmIdentifier;
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
    out.writeLong(this.containerId.getContainerId());
    out.writeUTF(this.nmHostAddr);
    out.writeUTF(this.appSubmitter);
    out.writeInt(this.resource.getMemory());
    out.writeInt(this.resource.getVirtualCores());
    out.writeLong(this.expiryTimeStamp);
    out.writeInt(this.masterKeyId);
    out.writeLong(this.rmIdentifier);
    out.writeInt(this.priority.getPriority());
    out.writeLong(this.creationTime);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    ApplicationId applicationId =
        ApplicationId.newInstance(in.readLong(), in.readInt());
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, in.readInt());
    this.containerId =
        ContainerId.newInstance(applicationAttemptId, in.readLong());
    this.nmHostAddr = in.readUTF();
    this.appSubmitter = in.readUTF();
    int memory = in.readInt();
    int vCores = in.readInt();
    this.resource = Resource.newInstance(memory, vCores);
    this.expiryTimeStamp = in.readLong();
    this.masterKeyId = in.readInt();
    this.rmIdentifier = in.readLong();
    this.priority = Priority.newInstance(in.readInt());
    this.creationTime = in.readLong();
  }

  @Override
  public Text getKind() {
    return KIND;
  }

  @Override
  public UserGroupInformation getUser() {
    return UserGroupInformation.createRemoteUser(this.containerId.toString());
  }

  // TODO: Needed?
  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND;
    }
  }
}
