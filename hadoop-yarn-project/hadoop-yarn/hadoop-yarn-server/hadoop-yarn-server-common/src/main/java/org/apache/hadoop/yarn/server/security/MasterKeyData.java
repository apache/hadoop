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

package org.apache.hadoop.yarn.server.security;

import java.nio.ByteBuffer;

import javax.crypto.SecretKey;

import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.util.Records;


public class MasterKeyData {

  private final MasterKey masterKeyRecord;
  // Underlying secret-key also stored to avoid repetitive encoding and
  // decoding the masterKeyRecord bytes.
  private final SecretKey generatedSecretKey;

  public MasterKeyData(int serialNo, SecretKey secretKey) {
    this.masterKeyRecord = Records.newRecord(MasterKey.class);
    this.masterKeyRecord.setKeyId(serialNo);
    this.generatedSecretKey = secretKey;
    this.masterKeyRecord.setBytes(ByteBuffer.wrap(generatedSecretKey
      .getEncoded()));
  }

  public MasterKeyData(MasterKey masterKeyRecord, SecretKey secretKey) {
    this.masterKeyRecord = masterKeyRecord;
    this.generatedSecretKey = secretKey;

  }

  public MasterKey getMasterKey() {
    return this.masterKeyRecord;
  }

  public SecretKey getSecretKey() {
    return this.generatedSecretKey;
  }
}

