/*
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

package org.apache.hadoop.fs.s3a.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.store.LogExactlyOnce;

import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SDK_V2_UPGRADE_LOG_NAME;

/**
 * This class provides utility methods required for migrating S3A to AWS Java SDK V2.
 * For more information on the upgrade, see HADOOP-18073.
 */
public final class V2Migration {

  private V2Migration() { }

  public static final Logger SDK_V2_UPGRADE_LOG = LoggerFactory.getLogger(SDK_V2_UPGRADE_LOG_NAME);

  private static final LogExactlyOnce WARN_ON_DELEGATION_TOKENS =
      new LogExactlyOnce(SDK_V2_UPGRADE_LOG);

  private static final LogExactlyOnce WARN_ON_GET_S3_CLIENT =
      new LogExactlyOnce(SDK_V2_UPGRADE_LOG);

  private static final LogExactlyOnce WARN_OF_DIRECTLY_REFERENCED_CREDENTIAL_PROVIDER =
      new LogExactlyOnce(SDK_V2_UPGRADE_LOG);

  private static final LogExactlyOnce WARN_OF_CUSTOM_SIGNER =
      new LogExactlyOnce(SDK_V2_UPGRADE_LOG);

  private static final LogExactlyOnce WARN_ON_GET_OBJECT_METADATA =
      new LogExactlyOnce(SDK_V2_UPGRADE_LOG);

  /**
   * Warns on an AWS V1 credential provider being referenced directly.
   * @param name name of the credential provider
   */
  public static void v1ProviderReferenced(String name) {
    WARN_OF_DIRECTLY_REFERENCED_CREDENTIAL_PROVIDER.warn(
        "Directly referencing AWS SDK V1 credential provider {}. AWS SDK V1 credential "
            + "providers will be removed once S3A is upgraded to SDK V2", name);
  }

  /**
   * Warns on the v1 s3 client being requested.
   */
  public static void v1S3ClientRequested() {
    WARN_ON_GET_S3_CLIENT.warn(
        "getAmazonS3ClientForTesting() will be removed as part of upgrading S3A to AWS SDK V2");
  }

  /**
   * Warns when v1 credential providers are used with delegation tokens.
   */
  public static void v1DelegationTokenCredentialProvidersUsed() {
    WARN_ON_DELEGATION_TOKENS.warn(
        "The credential provider interface has changed in AWS SDK V2, custom credential "
            + "providers used in delegation tokens binding classes will need to be updated once "
            + "S3A is upgraded to SDK V2");
  }

  /**
   * Warns on use of custom signers.
   */
  public static void v1CustomSignerUsed() {
    WARN_OF_CUSTOM_SIGNER.warn(
        "The signer interface has changed in AWS SDK V2, custom signers will need to be updated "
            + "once S3A is upgraded to SDK V2");
  }

  /**
   * Warns on use of getObjectMetadata.
   */
  public static void v1GetObjectMetadataCalled() {
    WARN_ON_GET_OBJECT_METADATA.warn("getObjectMetadata() called. This operation and it's response "
        + "will be changed as part of upgrading S3A to AWS SDK V2");
  }

}
