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

import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.Aws4UnsignedPayloadSigner;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.signer.NoOpSigner;
import software.amazon.awssdk.core.signer.Signer;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.fs.s3a.S3AUtils.getConstructor;
import static org.apache.hadoop.fs.s3a.S3AUtils.getFactoryMethod;
import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;

/**
 * Signer factory used to register and create signers.
 */
public final class SignerFactory {

  public static final String VERSION_FOUR_SIGNER = "AWS4SignerType";
  public static final String VERSION_FOUR_UNSIGNED_PAYLOAD_SIGNER = "AWS4UnsignedPayloadSignerType";
  public static final String NO_OP_SIGNER = "NoOpSignerType";
  private static final String S3_V4_SIGNER = "AWSS3V4SignerType";
  static final String INSTANTIATION_EXCEPTION
      = "instantiation exception";

  private static final Map<String, Class<? extends Signer>> SIGNERS
      = new HashMap<>();

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

    if (signerType == null) {
      throw new IllegalArgumentException("signerType cannot be null");
    }
    if (signerClass == null) {
      throw new IllegalArgumentException("signerClass cannot be null");
    }

    SIGNERS.put(signerType, signerClass);
  }

  /**
   * Check if the signer has already been registered.
   * @param signerType
   */
  public static void getSigner(String signerType) {
    Class<? extends Signer> signerClass = SIGNERS.get(signerType);
    if (signerClass == null) {
      throw new IllegalArgumentException("unknown signer type: " + signerType);
    }
  }


  /**
   * Create an instance of the given signer.
   *
   * @param signerType The signer type.
   *
   * @return The new signer instance.
   */
  public static Signer createSigner(String signerType) throws IOException {
    Class<? extends Signer> signerClass = SIGNERS.get(signerType);
    Signer signer = null;
    String className = signerClass.getName();
    try {
      // X.getInstance()
      Method factory = getFactoryMethod(signerClass, Signer.class,
          "create");
      if (factory != null) {
        signer = (Signer) factory.invoke(null);
        return signer;
      }

      // new X()
      Constructor cons = getConstructor(signerClass);
      if (cons != null) {
        signer = (Signer) cons.newInstance();
        return signer;
      }
    } catch (InvocationTargetException e) {
      // TODO: Can probably be moved to a common method, but before doing this, check if we still
      //  want to extend V2 providers the same way v1 providers are.
      Throwable targetException = e.getTargetException();
      if (targetException == null) {
        targetException =  e;
      }
      if (targetException instanceof IOException) {
        throw (IOException) targetException;
      } else if (targetException instanceof SdkException) {
        throw translateException("Instantiate " + className, "",
            (SdkException) targetException);
      } else {
        // supported constructor or factory method found, but the call failed
        throw new IOException(className + " " + INSTANTIATION_EXCEPTION
            + ": " + targetException,
            targetException);
      }
    } catch (ReflectiveOperationException | IllegalArgumentException e) {
      // supported constructor or factory method found, but the call failed
      throw new IOException(className + " " + INSTANTIATION_EXCEPTION
          + ": " + e,
          e);
    }
    return signer;
  }
}