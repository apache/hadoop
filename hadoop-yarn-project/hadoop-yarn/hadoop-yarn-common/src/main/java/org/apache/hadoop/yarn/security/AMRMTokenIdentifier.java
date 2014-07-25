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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * AMRMTokenIdentifier is the TokenIdentifier to be used by
 * ApplicationMasters to authenticate to the ResourceManager.
 */
@Public
@Evolving
public class AMRMTokenIdentifier extends TokenIdentifier {

  public static final Text KIND_NAME = new Text("YARN_AM_RM_TOKEN");

  private ApplicationAttemptId applicationAttemptId;
  private int keyId = Integer.MIN_VALUE;

  public AMRMTokenIdentifier() {
  }

  public AMRMTokenIdentifier(ApplicationAttemptId appAttemptId) {
    this();
    this.applicationAttemptId = appAttemptId;
  }

  public AMRMTokenIdentifier(ApplicationAttemptId appAttemptId,
      int masterKeyId) {
    this();
    this.applicationAttemptId = appAttemptId;
    this.keyId = masterKeyId;
  }

  @Private
  public ApplicationAttemptId getApplicationAttemptId() {
    return this.applicationAttemptId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    ApplicationId appId = this.applicationAttemptId.getApplicationId();
    out.writeLong(appId.getClusterTimestamp());
    out.writeInt(appId.getId());
    out.writeInt(this.applicationAttemptId.getAttemptId());
    out.writeInt(this.keyId);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    long clusterTimeStamp = in.readLong();
    int appId = in.readInt();
    int attemptId = in.readInt();
    ApplicationId applicationId =
        ApplicationId.newInstance(clusterTimeStamp, appId);
    this.applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, attemptId);
    this.keyId = in.readInt();
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  @Override
  public UserGroupInformation getUser() {
    if (this.applicationAttemptId == null
        || "".equals(this.applicationAttemptId.toString())) {
      return null;
    }
    return UserGroupInformation.createRemoteUser(this.applicationAttemptId
        .toString());
  }

  public int getKeyId() {
    return this.keyId;
  }

  // TODO: Needed?
  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND_NAME;
    }
  }
}
