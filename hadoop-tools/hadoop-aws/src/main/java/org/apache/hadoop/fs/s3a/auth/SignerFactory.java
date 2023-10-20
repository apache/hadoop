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

package org.apache.hadoop.fs.s3a.auth;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.Aws4UnsignedPayloadSigner;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.core.signer.NoOpSigner;
import software.amazon.awssdk.core.signer.Signer;

import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.impl.InstantiationIOException;

import static org.apache.hadoop.fs.s3a.impl.InstantiationIOException.unavailable;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Signer factory used to register and create signers.
 */
public final class SignerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SignerFactory.class);
  public static final String VERSION_FOUR_SIGNER = "AWS4SignerType";
  public static final String VERSION_FOUR_UNSIGNED_PAYLOAD_SIGNER = "AWS4UnsignedPayloadSignerType";
  public static final String NO_OP_SIGNER = "NoOpSignerType";
  private static final String S3_V4_SIGNER = "AWSS3V4SignerType";

  /** The v2 signer is no longer available: {@value}. */
  public static final String S3_V2_SIGNER = "S3SignerType";

  private static final Map<String, Class<? extends Signer>> SIGNERS
      = new ConcurrentHashMap<>();

  static {
    // Register the standard signer types.
    SIGNERS.put(VERSION_FOUR_SIGNER, Aws4Signer.class);
    SIGNERS.put(VERSION_FOUR_UNSIGNED_PAYLOAD_SIGNER, Aws4UnsignedPayloadSigner.class);
    SIGNERS.put(NO_OP_SIGNER, NoOpSigner.class);
    SIGNERS.put(S3_V4_SIGNER, AwsS3V4Signer.class);
  }


  private SignerFactory() {
  }

  /**
   * Register an implementation class for the given signer type.
   *
   * @param signerType  The name of the signer type to register.
   * @param signerClass The class implementing the given signature protocol.
   */
  public static void registerSigner(
      final String signerType,
      final Class<? extends Signer> signerClass) {

    checkArgument(signerType != null, "signerType cannot be null");
    checkArgument(signerClass != null, "signerClass cannot be null");

    SIGNERS.put(signerType, signerClass);
  }

  /**
   * Check if the signer has already been registered.
   * @param signerType signer to get
   * @throws IllegalArgumentException if the signer type is unknown.
   */
  public static void verifySignerRegistered(String signerType) {
    checkArgument(isSignerRegistered(signerType),
        "unknown signer type: %s", signerType);
  }

  /**
   * Check if the signer has already been registered.
   * @param signerType signer to get
   * @return true if the signer is registered.
   */
  public static boolean isSignerRegistered(String signerType) {
    return SIGNERS.containsKey(signerType);
  }

  /**
   * Create an instance of the given signer.
   *
   * @param signerType The signer type.
   * @param configKey Config key used to configure the signer.
   * @return The new signer instance.
   * @throws InstantiationIOException instantiation problems.
   * @throws IOException on any other problem.
   *
   */
  public static Signer createSigner(String signerType, String configKey) throws IOException {
    if (S3_V2_SIGNER.equals(signerType)) {
      throw unavailable(null, null, configKey, S3_V2_SIGNER + " is no longer supported");
    }
    if (!isSignerRegistered(signerType)) {
      throw unavailable(null, null, configKey, "unknown signer type: " + signerType);
    }
    Class<?> signerClass = SIGNERS.get(signerType);

    String className = signerClass.getName();

    LOG.debug("Signer class from {} and key {} is {}", signerType, configKey, className);

    Signer signer =
        S3AUtils.getInstanceFromReflection(className, null, null, Signer.class, "create",
            configKey);

    return signer;
  }
}
