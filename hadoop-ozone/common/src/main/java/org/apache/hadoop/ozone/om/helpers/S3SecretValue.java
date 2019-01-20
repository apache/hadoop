/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.io.IOException;

/**
 * S3Secret to be saved in database.
 */
public class S3SecretValue {
  private String kerberosID;
  private String awsSecret;
  private String awsAccessKey;

  public S3SecretValue(String kerberosID, String awsSecret) throws IOException {
    this.kerberosID = kerberosID;
    this.awsSecret = awsSecret;
    this.awsAccessKey =
        DigestUtils.md5Hex(OmUtils.getMD5Digest(kerberosID));
  }

  public String getKerberosID() {
    return kerberosID;
  }

  public void setKerberosID(String kerberosID) {
    this.kerberosID = kerberosID;
  }

  public String getAwsSecret() {
    return awsSecret;
  }

  public void setAwsSecret(String awsSecret) {
    this.awsSecret = awsSecret;
  }

  public String getAwsAccessKey() {
    return awsAccessKey;
  }

  public void setAwsAccessKey(String awsAccessKey) {
    this.awsAccessKey = awsAccessKey;
  }

  public static S3SecretValue fromProtobuf(
      OzoneManagerProtocolProtos.S3Secret s3Secret) throws IOException {
    return new S3SecretValue(s3Secret.getKerberosID(), s3Secret.getAwsSecret());
  }

  public OzoneManagerProtocolProtos.S3Secret getProtobuf() {
    return OzoneManagerProtocolProtos.S3Secret.newBuilder()
        .setAwsSecret(this.awsSecret)
        .setKerberosID(this.kerberosID)
        .build();
  }

  @Override
  public String toString() {
    return "awsAccessKey=" + awsAccessKey + "\nawsSecret=" + awsSecret;
  }
}
