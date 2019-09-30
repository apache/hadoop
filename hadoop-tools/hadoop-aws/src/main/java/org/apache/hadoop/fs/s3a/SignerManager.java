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
package org.apache.hadoop.fs.s3a;

import com.amazonaws.auth.Signer;
import com.amazonaws.auth.SignerFactory;
import java.io.Closeable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.Constants.CUSTOM_SIGNERS;

/**
 * Class to handle custom signers.
 */
public class SignerManager implements Closeable {

  private static final Logger LOG = LoggerFactory
      .getLogger(SignerManager.class);


  public SignerManager() {
  }

  /**
   * Initialize custom signers and register them with the AWS SDK.
   *
   * @param conf Hadoop configuration
   */
  public void initCustomSigners(Configuration conf) {
    String[] customSigners = conf.getTrimmedStrings(CUSTOM_SIGNERS);
    if (customSigners == null || customSigners.length == 0) {
      // No custom signers specified, nothing to do.
      LOG.debug("No custom signers specified");
      return;
    }

    for (String customSigner : customSigners) {
      String[] parts = customSigner.split(":");
      if (parts.length != 2) {
        String message =
            "Invalid format (Expected name:SignerClass) for CustomSigner: ["
                + customSigner
                + "]";
        LOG.error(message);
        throw new IllegalArgumentException(message);
      }
      maybeRegisterSigner(parts[0], parts[1], conf);
    }
  }

  /*
   * Make sure the signer class is registered once with the AWS SDK
   */
  private static void maybeRegisterSigner(String signerName,
      String signerClassName, Configuration conf) {
    try {
      SignerFactory.getSignerByTypeAndService(signerName, null);
    } catch (IllegalArgumentException e) {
      // Signer is not registered with the AWS SDK.
      // Load the class and register the signer.
      Class<? extends Signer> clazz = null;
      try {
        clazz = (Class<? extends Signer>) conf.getClassByName(signerClassName);
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException(String
            .format("Signer class [%s] not found for signer [%s]",
                signerClassName, signerName), cnfe);
      }
      LOG.debug("Registering Custom Signer - [{}->{}]", signerName,
          clazz.getName());
      synchronized (SignerManager.class) {
        SignerFactory.registerSigner(signerName, clazz);
      }
    }
  }

  @Override
  public void close() throws IOException {
  }
}
