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

package org.apache.hadoop.yarn.security.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.BuilderUtils;

public class ClientTokenIdentifier extends TokenIdentifier {

  public static final Text KIND_NAME = new Text("YARN_CLIENT_TOKEN");

  private ApplicationId applicationId;

  // TODO: Add more information in the tokenID such that it is not
  // transferrable, more secure etc.

  public ClientTokenIdentifier() {
  }

  public ClientTokenIdentifier(ApplicationId id) {
    this();
    this.applicationId = id;
  }

  public ApplicationId getApplicationID() {
    return this.applicationId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(this.applicationId.getClusterTimestamp());
    out.writeInt(this.applicationId.getId());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.applicationId =
        BuilderUtils.newApplicationId(in.readLong(), in.readInt());
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  @Override
  public UserGroupInformation getUser() {
    if (this.applicationId == null) {
      return null;
    }
    return UserGroupInformation.createRemoteUser(this.applicationId.toString());
  }

  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND_NAME;
    }
  }
}
