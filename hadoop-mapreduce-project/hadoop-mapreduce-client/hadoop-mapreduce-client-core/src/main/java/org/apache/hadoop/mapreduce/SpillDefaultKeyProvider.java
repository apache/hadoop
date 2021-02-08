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
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.KeyGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;

/**
 * The implementation uses keygenerator to encrypt data spilled to disk.
 */
public class SpillDefaultKeyProvider extends SpillNullKeyProvider {

  private int keyLength;
  private String keyAlgo;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) {
      return;
    }
    setEncryptionEnabled(getConf().getBoolean(
        MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA,
        MRJobConfig.DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA));
    setKeyLen(getConf().getInt(
        MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS,
        MRJobConfig.DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS));
    setKeyAlgo(getConf().get(
        MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA_KEYPROVIDER_ALGORITHM,
        MRJobConfig
            .DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_KEYPROVIDER_ALGORITHM));
  }

  @Override
  public byte[] getEncryptionSpillKey(
      Credentials credentials) throws IOException {
    KeyGenerator keyGen;
    try {
      keyGen = KeyGenerator.getInstance(getKeyAlgo());
      keyGen.init(getKeyLength());
      return keyGen.generateKey().getEncoded();
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Error generating encrypted spill key", e);
    }
  }

  protected void setKeyLen(int length) {
    keyLength = length;
  }

  public int getKeyLength() {
    return keyLength;
  }

  protected void setKeyAlgo(String algo) {
    keyAlgo = algo;
  }

  public String getKeyAlgo() {
    return keyAlgo;
  }
}
