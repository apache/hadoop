/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.OzoneSecurityException;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.security.OzoneSecurityException.ResultCodes.S3_SECRET_NOT_FOUND;

/**
 * S3 Secret manager.
 */
public class S3SecretManagerImpl implements S3SecretManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(S3SecretManagerImpl.class);

  /**
   * OMMetadataManager is used for accessing OM MetadataDB and ReadWriteLock.
   */
  private final OMMetadataManager omMetadataManager;
  private final OzoneConfiguration configuration;

  /**
   * Constructs S3SecretManager.
   *
   * @param omMetadataManager
   */
  public S3SecretManagerImpl(OzoneConfiguration configuration,
      OMMetadataManager omMetadataManager) {
    this.configuration = configuration;
    this.omMetadataManager = omMetadataManager;
  }

  @Override
  public S3SecretValue getS3Secret(String kerberosID) throws IOException {
    Preconditions.checkArgument(Strings.isNotBlank(kerberosID),
        "kerberosID cannot be null or empty.");
    String awsAccessKeyStr = DigestUtils.md5Hex(kerberosID);
    byte[] awsAccessKey = awsAccessKeyStr.getBytes(UTF_8);
    S3SecretValue result = null;
    omMetadataManager.getLock().acquireS3SecretLock(kerberosID);
    try {
      byte[] s3Secret =
          omMetadataManager.getS3SecretTable().get(awsAccessKey);
      if(s3Secret == null) {
        byte[] secret = OmUtils.getSHADigest();
        result = new S3SecretValue(kerberosID, DigestUtils.sha256Hex(secret));
        omMetadataManager.getS3SecretTable()
            .put(awsAccessKey, result.getProtobuf().toByteArray());
      } else {
        result = S3SecretValue.fromProtobuf(
            OzoneManagerProtocolProtos.S3Secret.parseFrom(s3Secret));
      }
      result.setAwsAccessKey(awsAccessKeyStr);
    } finally {
      omMetadataManager.getLock().releaseS3SecretLock(kerberosID);
    }
    LOG.trace("Secret for kerberosID:{},accessKey:{}, proto:{}", kerberosID,
        awsAccessKeyStr, result);
    return result;
  }

  @Override
  public String getS3UserSecretString(String awsAccessKeyId)
      throws IOException {
    Preconditions.checkArgument(Strings.isNotBlank(awsAccessKeyId),
        "awsAccessKeyId cannot be null or empty.");
    LOG.trace("Get secret for awsAccessKey:{}", awsAccessKeyId);

    byte[] s3Secret;
    omMetadataManager.getLock().acquireS3SecretLock(awsAccessKeyId);
    try {
      s3Secret = omMetadataManager.getS3SecretTable()
          .get(awsAccessKeyId.getBytes(UTF_8));
      if (s3Secret == null) {
        throw new OzoneSecurityException("S3 secret not found for " +
            "awsAccessKeyId " + awsAccessKeyId, S3_SECRET_NOT_FOUND);
      }
    } finally {
      omMetadataManager.getLock().releaseS3SecretLock(awsAccessKeyId);
    }

    return OzoneManagerProtocolProtos.S3Secret.parseFrom(s3Secret)
        .getAwsSecret();
  }
}
