/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.protobuf.TextFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.DockerCredentialTokenIdentifierProto;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TokenIdentifier for Docker registry credentials.
 */
public class DockerCredentialTokenIdentifier extends TokenIdentifier {

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(DockerCredentialTokenIdentifier.class);

  private DockerCredentialTokenIdentifierProto proto;
  public static final Text KIND = new Text("DOCKER_CLIENT_CREDENTIAL_TOKEN");

  public DockerCredentialTokenIdentifier(String registryUrl,
      String applicationId) {
    DockerCredentialTokenIdentifierProto.Builder builder =
        DockerCredentialTokenIdentifierProto.newBuilder();
    if (registryUrl != null) {
      builder.setRegistryUrl(registryUrl);
    }
    if (applicationId != null) {
      builder.setApplicationId(applicationId);
    }
    proto = builder.build();
  }

  /**
   * Default constructor needed for the Service Loader.
   */
  public DockerCredentialTokenIdentifier() {
  }

  /**
   * Write the TokenIdentifier to the output stream.
   *
   * @param out <code>DataOutput</code> to serialize this object into.
   * @throws IOException if the write fails.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.write(proto.toByteArray());
  }

  /**
   * Populate the Proto object with the input.
   *
   * @param in <code>DataInput</code> to deserialize this object from.
   * @throws IOException if the read fails.
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    proto = DockerCredentialTokenIdentifierProto.parseFrom((DataInputStream)in);
  }

  /**
   * Return the ProtoBuf formatted data.
   *
   * @return the ProtoBuf representation of the data.
   */
  public DockerCredentialTokenIdentifierProto getProto() {
    return proto;
  }

  /**
   * Return the TokenIdentifier kind.
   *
   * @return the TokenIdentifier kind.
   */
  @Override
  public Text getKind() {
    return KIND;
  }

  /**
   * Return a remote user based on the registry URL and Application ID.
   *
   * @return a remote user based on the registry URL and Application ID.
   */
  @Override
  public UserGroupInformation getUser() {
    return UserGroupInformation.createRemoteUser(
        getRegistryUrl() + "-" + getApplicationId());
  }

  /**
   * Get the registry URL.
   *
   * @return the registry URL.
   */
  public String getRegistryUrl() {
    String registryUrl = null;
    if (proto.hasRegistryUrl()) {
      registryUrl = proto.getRegistryUrl();
    }
    return registryUrl;
  }

  /**
   * Get the application ID.
   *
   * @return the application ID.
   */
  public String getApplicationId() {
    String applicationId = null;
    if (proto.hasApplicationId()) {
      applicationId = proto.getApplicationId();
    }
    return applicationId;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}
