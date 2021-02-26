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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.Credentials;

/**
 * The implementation that provides no encryption to date spilled to disk.
 */
public class SpillNullKeyProvider
    extends Configured implements SpillKeyProvider {
  private boolean encryptionEnabled;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    setEncryptionEnabled(false);
  }

  @Override
  public void addEncryptedSpillKey(Credentials credentials)
      throws IOException {
    // do nothing
  }

  @Override
  public byte[] getEncryptionSpillKey(Credentials credentials)
      throws IOException {
    return new byte[] {0};
  }

  @Override
  public boolean isEncryptionEnabled() {
    return encryptionEnabled;
  }

  public void setEncryptionEnabled(boolean encryptionEnabled) {
    this.encryptionEnabled = encryptionEnabled;
  }
}
